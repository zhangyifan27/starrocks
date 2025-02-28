// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.iceberg;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.glue.IcebergGlueCatalog;
import com.starrocks.connector.iceberg.hadoop.IcebergHadoopCatalog;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utils.TAuthUtils;
import com.starrocks.utils.TdwUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.ThreadPools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.iceberg.util.ThreadPools.newWorkerPool;

public class IcebergConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(IcebergConnector.class);
    private static final long ICEBERG_REST_CATALOG_TABLE_CACHE_TTL_S = 30 * 60L;
    public static final String HADOOP_TAUTH_USER = "hadoop.tauth.user";
    public static final String HADOOP_TAUTH_KEY = "hadoop.tauth.key";
    public static final String HADOOP_TAUTH_PROXY_USER = "hadoop.tauth.proxyuser";
    private final Map<String, String> properties;
    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private IcebergCatalog icebergNativeCatalog;
    private IcebergCatalogType nativeCatalogType;
    private ExecutorService icebergJobPlanningExecutor;
    private ExecutorService refreshOtherFeExecutor;
    private final IcebergCatalogProperties icebergCatalogProperties;
    private Cache<TableIdentifier, IcebergTable> icebergTableCache;
    private com.github.benmanes.caffeine.cache.Cache<String, IcebergCatalog> icebergCatalogCache;

    public IcebergConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
        this.icebergCatalogProperties = new IcebergCatalogProperties(properties);
        init();
    }

    private void init() {
        nativeCatalogType = icebergCatalogProperties.getCatalogType();
        if (Config.enable_iceberg_custom_worker_thread) {
            LOG.info("Default iceberg worker thread number changed " + Config.iceberg_worker_num_threads);
            Properties props = System.getProperties();
            props.setProperty(ThreadPools.WORKER_THREAD_POOL_SIZE_PROP,
                    String.valueOf(Config.iceberg_worker_num_threads));
        }

        long icebergTableCacheTTL = Config.hive_meta_cache_ttl_s;
        if (nativeCatalogType == IcebergCatalogType.REST_CATALOG) {
            icebergTableCacheTTL = ICEBERG_REST_CATALOG_TABLE_CACHE_TTL_S;
        }
        long icebergCatalogCacheTTL = Config.hive_meta_cache_ttl_s;
        this.icebergTableCache = CacheBuilder.newBuilder().expireAfterWrite(icebergTableCacheTTL, SECONDS)
                .maximumSize(1000000).build();
        this.icebergCatalogCache = Caffeine.newBuilder()
                .expireAfterAccess(icebergCatalogCacheTTL, SECONDS)
                .maximumSize(100000)
                .removalListener(new IcebergCatalogRemovalListener())
                .scheduler(Scheduler.forScheduledExecutorService(
                        ThreadPools.newScheduledPool("iceberg-catalog-cleaner", 1)))
                .build();
    }

    private IcebergCatalog buildIcebergNativeCatalog() {
        Configuration conf = hdfsEnvironment.getConfiguration();

        switch (nativeCatalogType) {
            case HIVE_CATALOG:
                return new IcebergHiveCatalog(catalogName, conf, properties);
            case GLUE_CATALOG:
                return new IcebergGlueCatalog(catalogName, conf, properties);
            case REST_CATALOG:
                return new IcebergRESTCatalog(catalogName, conf, properties);
            case HADOOP_CATALOG:
                return new IcebergHadoopCatalog(catalogName, conf, properties);
            default:
                throw new StarRocksConnectorException("Property %s is missing or not supported now.", ICEBERG_CATALOG_TYPE);
        }
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new IcebergMetadata(catalogName, hdfsEnvironment, getNativeCatalog(TdwUtil.getTdwUserName()),
                buildIcebergJobPlanningExecutor(), buildRefreshOtherFeExecutor(), icebergCatalogProperties);
    }

    // In order to be compatible with the catalog created with the wrong configuration,
    // icebergNativeCatalog is lazy, mainly to prevent fe restart failure.
    public IcebergCatalog getNativeCatalog() {
        if (icebergNativeCatalog == null) {
            IcebergCatalog nativeCatalog = buildIcebergNativeCatalog();

            if (icebergCatalogProperties.enableIcebergMetadataCache() && !isResourceMappingCatalog(catalogName)) {
                nativeCatalog = new CachingIcebergCatalog(catalogName, nativeCatalog,
                        icebergCatalogProperties, buildBackgroundJobPlanningExecutor());
                GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor()
                        .registerCachingIcebergCatalog(catalogName, nativeCatalog);
            }
            this.icebergNativeCatalog = nativeCatalog;
        }
        return icebergNativeCatalog;
    }

    public IcebergCatalog getNativeCatalog(String username) {
        IcebergCatalog icebergCatalog = icebergCatalogCache.get(username, this::loadIcebergNativeCatalog);
        return icebergCatalog;
    }

    private IcebergCatalog loadIcebergNativeCatalog(String username) {
        Configuration conf = new Configuration(hdfsEnvironment.getConfiguration());
        conf.set(HADOOP_TAUTH_USER, TAuthUtils.getTauthPlatformUser());
        conf.set(HADOOP_TAUTH_KEY, TAuthUtils.getTauthPlatformCMK());
        conf.set(HADOOP_TAUTH_PROXY_USER, username);
        LOG.info("Create IcebergCatalog for user {}", username);
        switch (nativeCatalogType) {
            case HIVE_CATALOG:
                return new IcebergHiveCatalog(catalogName, conf, properties);
            case GLUE_CATALOG:
                return new IcebergGlueCatalog(catalogName, conf, properties);
            case REST_CATALOG:
                return new IcebergRESTCatalog(catalogName, conf, properties);
            case HADOOP_CATALOG:
                return new IcebergHadoopCatalog(catalogName, conf, properties);
            default:
                throw new StarRocksConnectorException("Property %s is missing or not supported now.",
                        ICEBERG_CATALOG_TYPE);
        }
    }

    class IcebergCatalogRemovalListener
            implements RemovalListener<String, IcebergCatalog> {
        @Override
        public void onRemoval(String username, IcebergCatalog catalog, RemovalCause cause) {
            try {
                LOG.info("close IcebergCatalog {} for user {} ", catalog, username);
                catalog.close();
            } catch (Exception e) {
                LOG.warn("close IcebergCatalog error", e);
            }
        }
    }

    private ExecutorService buildIcebergJobPlanningExecutor() {
        if (icebergJobPlanningExecutor == null) {
            icebergJobPlanningExecutor = newWorkerPool(catalogName + "-sr-iceberg-worker-pool",
                    icebergCatalogProperties.getIcebergJobPlanningThreadNum());
        }

        return icebergJobPlanningExecutor;
    }

    public ExecutorService buildRefreshOtherFeExecutor() {
        if (refreshOtherFeExecutor == null) {
            refreshOtherFeExecutor = newWorkerPool(catalogName + "-refresh-others-fe-iceberg-metadata-cache",
                    icebergCatalogProperties.getRefreshOtherFeIcebergCacheThreadNum());
        }
        return refreshOtherFeExecutor;
    }

    private ExecutorService buildBackgroundJobPlanningExecutor() {
        return newWorkerPool(catalogName + "-background-iceberg-worker-pool",
                icebergCatalogProperties.getBackgroundIcebergJobPlanningThreadNum());
    }

    @Override
    public void shutdown() {
        GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor().unRegisterCachingIcebergCatalog(catalogName);
        if (icebergJobPlanningExecutor != null) {
            icebergJobPlanningExecutor.shutdown();
        }
        if (refreshOtherFeExecutor != null) {
            refreshOtherFeExecutor.shutdown();
        }
    }

    @Override
    public boolean supportMemoryTrack() {
        return icebergCatalogProperties.enableIcebergMetadataCache() && icebergNativeCatalog != null;
    }

    @Override
    public Map<String, Long> estimateCount() {
        return icebergNativeCatalog.estimateCount();
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        return icebergNativeCatalog.getSamples();
    }
}
