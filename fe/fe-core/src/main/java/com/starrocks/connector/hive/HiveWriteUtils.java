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

package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.HiveTable;
import com.starrocks.common.DdlException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utils.TAuthUtils;
import com.tencent.tdw.security.exceptions.SecureException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.UUID;

import static com.starrocks.connector.hive.HiveMetastoreOperations.EXTERNAL_LOCATION_PROPERTY;

public class HiveWriteUtils {
    private static final Logger LOG = LogManager.getLogger(HiveWriteUtils.class);
    public static boolean isS3Url(String prefix) {
        return prefix.startsWith("oss://") || prefix.startsWith("s3n://") || prefix.startsWith("s3a://") ||
                prefix.startsWith("s3://") || prefix.startsWith("cos://") || prefix.startsWith("cosn://") ||
                prefix.startsWith("obs://") || prefix.startsWith("ks3://") || prefix.startsWith("tos://");
    }

    public static void checkLocationProperties(Map<String, String> properties) throws DdlException {
        if (properties.containsKey(EXTERNAL_LOCATION_PROPERTY)) {
            throw new DdlException("Can't create non-managed Hive table. " +
                    "Only supports creating hive table under Database location. " +
                    "You could execute command without external_location properties");
        }
    }

    public static boolean pathExists(Path path, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            return fileSystem.exists(path);
        } catch (Exception e) {
            LOG.error("Failed to check path {}", path, e);
            throw new StarRocksConnectorException("Failed to check path: " + path + ". msg: " + e.getMessage());
        }
    }

    public static boolean isDirectory(Path path, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            return fileSystem.getFileStatus(path).isDirectory();
        } catch (IOException e) {
            LOG.error("Failed checking path {}", path, e);
            throw new StarRocksConnectorException("Failed checking path: " + path);
        }
    }

    public static boolean isEmpty(Path path, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            return !fileSystem.listFiles(path, false).hasNext();
        } catch (IOException e) {
            LOG.error("Failed checking path {}", path, e);
            throw new StarRocksConnectorException("Failed checking path: " + path);
        }
    }

    public static FileSystem getTAuthFileSystem(Path path, Configuration conf) throws IOException, InterruptedException,
            SecureException {
        String proxyUser = "";
        if (ConnectContext.get() != null && !Strings.isNullOrEmpty(ConnectContext.get().getQualifiedUser())) {
            proxyUser = ConnectContext.get().getQualifiedUser();
        }
        if (proxyUser.equals("root")) {
            proxyUser = TAuthUtils.getDefaultTdwUser();
        }
        UserGroupInformation ugi = TAuthUtils.getPlatformUser();
        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUser(proxyUser, ugi);
        conf.set("hadoop.security.authentication", "TAUTH");
        return proxyUgi.doAs((PrivilegedExceptionAction<FileSystem>) () ->
                FileSystem.get(path.toUri(), conf));
    }

    public static void createDirectory(Path path, Configuration conf) {
        try {
            FileSystem fileSystem = getTAuthFileSystem(path, conf);
            if (!fileSystem.mkdirs(path)) {
                LOG.error("Mkdir {} returned false", path);
                throw new IOException("mkdirs returned false");
            }
        } catch (IOException e) {
            LOG.error("Failed to create directory: {}", path);
            throw new StarRocksConnectorException("Failed to create directory: " + path, e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (SecureException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getStagingDir(HiveTable table, String tempStagingDir) {
        String stagingDir;
        String location = table.getTableLocation();
        if (isS3Url(location)) {
            stagingDir = location;
        } else {
            Path tempRoot = Path.mergePaths(new Path(location).getParent(), new Path(tempStagingDir));
            Path tempStagingPath = new Path(tempRoot, UUID.randomUUID().toString());
            stagingDir = tempStagingPath.toString();
        }
        return stagingDir.endsWith("/") ? stagingDir : stagingDir + "/";
    }

    public static boolean fileCreatedByQuery(String fileName, String queryId) {
        Preconditions.checkState(queryId.length() > 8, "file name or query id is invalid");
        if (fileName.length() <= queryId.length()) {
            // file is created by other engine like hive
            return false;
        }
        String checkQueryId = queryId.substring(0, queryId.length() - 8);
        return fileName.startsWith(checkQueryId) || fileName.endsWith(checkQueryId);
    }

    public static void checkedDelete(FileSystem fileSystem, Path file, boolean recursive) throws IOException {
        try {
            if (!fileSystem.delete(file, recursive)) {
                if (fileSystem.exists(file)) {
                    throw new IOException("Failed to delete " + file);
                }
            }
        } catch (FileNotFoundException ignored) {
            // ignore
        }
    }

    public static boolean deleteIfExists(Path path, boolean recursive, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            if (fileSystem.delete(path, recursive)) {
                return true;
            }

            return !fileSystem.exists(path);
        } catch (FileNotFoundException ignored) {
            return true;
        } catch (IOException ignored) {
            LOG.error("Failed to delete remote path {}", path);
        }

        return false;
    }

}
