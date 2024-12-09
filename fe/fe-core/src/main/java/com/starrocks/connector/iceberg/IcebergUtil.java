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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/ha/BDBHA.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.IcebergResource;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName;

public class IcebergUtil {
    private static final ConcurrentHashMap<String, IcebergHiveCatalog> METASTORE_URI_TO_CATALOG =
            new ConcurrentHashMap<>();

    private static synchronized IcebergHiveCatalog getIcebergHiveCatalogInstance(String uri,
                                                                                 Map<String, String> properties,
                                                                                 HdfsEnvironment hdfsEnvironment) {
        if (!METASTORE_URI_TO_CATALOG.containsKey(uri)) {
            properties.put(IcebergCatalogProperties.HIVE_METASTORE_URIS, uri);
            properties.put(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive");
            METASTORE_URI_TO_CATALOG.put(uri, new IcebergHiveCatalog(String.format("hive-%s", uri),
                    hdfsEnvironment.getConfiguration(), properties));
        }
        return METASTORE_URI_TO_CATALOG.get(uri);
    }

    public static org.apache.iceberg.Table getTableFromHiveMetastore(String metastoreUris, String dbName,
                                                                     String tblName) throws StarRocksConnectorException {
        IcebergHiveCatalog catalog =
                getIcebergHiveCatalogInstance(metastoreUris, new HashMap<>(), new HdfsEnvironment());
        return catalog.getTable(dbName, tblName);
    }

    public static Table getTableFromResource(String resourceName, String dbName, String tblName)
            throws StarRocksConnectorException, AnalysisException {
        ResourceMgr resourceMgr = GlobalStateMgr.getCurrentState().getResourceMgr();
        Resource resource = resourceMgr.getResource(resourceName);
        if (resource == null) {
            throw new AnalysisException("Resource " + resourceName + " not exists");
        }
        if (!(resource instanceof IcebergResource)) {
            throw new AnalysisException("Resource " + resourceName + " is not iceberg resource");
        }

        String type = resource.getType().name().toLowerCase(Locale.ROOT);

        String catalogName = getResourceMappingCatalogName(resource.getName(), type);
        return getTableFromCatalog(catalogName, dbName, tblName);
    }

    public static Table getTableFromCatalog(String catalogName, String dbName, String tblName)
            throws StarRocksConnectorException, AnalysisException {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        IcebergTable resTable = (IcebergTable) metadataMgr.getTable(catalogName, dbName, tblName);
        if (resTable == null) {
            throw new StarRocksConnectorException(catalogName + "." + dbName + "." + tblName + " not exists");
        } else {
            return resTable.getNativeTable();
        }
    }

    public static boolean isMorTable(BaseTable table) {
        if (table.operations().current().formatVersion() != 2) {
            return false;
        }
        return RowLevelOperationMode.MERGE_ON_READ.modeName().equals(table.properties().get(TableProperties.UPSERT_ENABLED))
                || RowLevelOperationMode.MERGE_ON_READ.modeName().equals(table.properties().get(TableProperties.DELETE_MODE))
                || RowLevelOperationMode.MERGE_ON_READ.modeName().equals(table.properties().get(TableProperties.UPDATE_MODE))
                || RowLevelOperationMode.MERGE_ON_READ.modeName().equals(table.properties().get(TableProperties.MERGE_MODE));
    }

    public static boolean isCowTable(BaseTable table) {
        if (table.operations().current().formatVersion() != 2) {
            return false;
        }
        return RowLevelOperationMode.COPY_ON_WRITE.modeName().equals(table.properties().get(TableProperties.UPSERT_ENABLED))
                || RowLevelOperationMode.COPY_ON_WRITE.modeName().equals(table.properties().get(TableProperties.DELETE_MODE))
                || RowLevelOperationMode.COPY_ON_WRITE.modeName().equals(table.properties().get(TableProperties.UPDATE_MODE))
                || RowLevelOperationMode.COPY_ON_WRITE.modeName().equals(table.properties().get(TableProperties.MERGE_MODE));
    }
}