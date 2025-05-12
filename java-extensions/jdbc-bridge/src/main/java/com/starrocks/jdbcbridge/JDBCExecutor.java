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

package com.starrocks.jdbcbridge;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class JDBCExecutor {
    private String driverLocation;
    private JDBCScanContext scanContext;
    private HikariDataSource dataSource;
    private Connection connection;
    private PreparedStatement preparedStatement = null;
    private ClassLoader classLoader;

    public JDBCExecutor(String driverLocation, JDBCScanContext scanContext) {
        this.driverLocation = driverLocation;
        this.scanContext = scanContext;
    }

    public void open() throws Exception {
        String key = scanContext.getUser() + "/" + scanContext.getJdbcURL();
        URL driverURL = new File(driverLocation).toURI().toURL();
        DataSourceCache.DataSourceCacheItem cacheItem = DataSourceCache.getInstance().getSource(key, () -> {
            ClassLoader classLoader = URLClassLoader.newInstance(new URL[] {
                    driverURL,
            });
            Thread.currentThread().setContextClassLoader(classLoader);
            HikariConfig config = new HikariConfig();
            config.setDriverClassName(scanContext.getDriverClassName());
            config.setJdbcUrl(scanContext.getJdbcURL());
            config.setUsername(scanContext.getUser());
            config.setPassword(scanContext.getPassword());
            config.setMaximumPoolSize(scanContext.getConnectionPoolSize());
            config.setMinimumIdle(scanContext.getMinimumIdleConnections());
            config.setIdleTimeout(scanContext.getConnectionIdleTimeoutMs());
            HikariDataSource hikariDataSource = new HikariDataSource(config);
            return new DataSourceCache.DataSourceCacheItem(hikariDataSource, classLoader);
        });
        dataSource = cacheItem.getHikariDataSource();
        classLoader = cacheItem.getClassLoader();

        connection = dataSource.getConnection();
        if (scanContext.getSql() != null && !scanContext.getSql().isEmpty()) {
            preparedStatement = connection.prepareStatement(scanContext.getSql());
        }
    }

    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    public int write(int[] columnTypes, Object[][] data) throws SQLException {
        if (preparedStatement == null) {
            throw new RuntimeException("SQL String cannot be empty for writing case");
        }
        int numRows = data[0].length;
        for (int row = 0; row < numRows; row++) {
            for (int col = 0; col < data.length; col++) {
                writeSingleData(row, col, columnTypes[col], data[col]);
            }

            preparedStatement.addBatch();
        }

        preparedStatement.executeBatch();
        preparedStatement.clearBatch();
        return numRows;
    }

    public int executeRaw(String sql) throws SQLException {
        Statement singleStmt = connection.createStatement();
        return singleStmt.executeUpdate(sql);
    }

    private void writeSingleData(int row, int col, int colType, Object[] column)
            throws SQLException {
        int stmtIndex = col + 1;
        boolean isNull = column[row] == null;
        switch (colType) {
            case ColumnType.TYPE_BOOLEAN:
                if (isNull) {
                    preparedStatement.setNull(stmtIndex, Types.BOOLEAN);
                } else {
                    preparedStatement.setBoolean(stmtIndex, (Boolean) column[row]);
                }
                break;
            case ColumnType.TYPE_TINYINT:
                if (isNull) {
                    preparedStatement.setNull(stmtIndex, Types.TINYINT);
                } else {
                    preparedStatement.setByte(stmtIndex, ((Integer) column[row]).byteValue());
                }
                break;
            case ColumnType.TYPE_SMALLINT:
                if (isNull) {
                    preparedStatement.setNull(stmtIndex, Types.SMALLINT);
                } else {
                    preparedStatement.setShort(stmtIndex, ((Integer) column[row]).shortValue());
                }
                break;
            case ColumnType.TYPE_INT:
                if (isNull) {
                    preparedStatement.setNull(stmtIndex, Types.INTEGER);
                } else {
                    preparedStatement.setInt(stmtIndex, (Integer) column[row]);
                }
                break;
            case ColumnType.TYPE_BIGINT:
                if (isNull) {
                    preparedStatement.setNull(stmtIndex, Types.BIGINT);
                } else {
                    preparedStatement.setLong(stmtIndex, (Long) column[row]);
                }
                break;
            case ColumnType.TYPE_LARGEINT:
                if (isNull) {
                    preparedStatement.setNull(stmtIndex, Types.JAVA_OBJECT);
                } else {
                    preparedStatement.setObject(stmtIndex, (BigInteger) column[row]);
                }
                break;
            case ColumnType.TYPE_FLOAT:
                if (isNull) {
                    preparedStatement.setNull(stmtIndex, Types.FLOAT);
                } else {
                    preparedStatement.setFloat(stmtIndex, (Float) column[row]);
                }
                break;
            case ColumnType.TYPE_DOUBLE:
                if (isNull) {
                    preparedStatement.setNull(stmtIndex, Types.DOUBLE);
                } else {
                    preparedStatement.setDouble(stmtIndex, (Double) column[row]);
                }
                break;
            case ColumnType.TYPE_DECIMAL32:
            case ColumnType.TYPE_DECIMAL64:
            case ColumnType.TYPE_DECIMAL128:
            case ColumnType.TYPE_DECIMALV2:
                if (isNull) {
                    preparedStatement.setNull(stmtIndex, Types.DECIMAL);
                } else {
                    preparedStatement.setBigDecimal(stmtIndex, (BigDecimal) column[row]);
                }
                break;
            case ColumnType.TYPE_DATE:
                if (isNull) {
                    preparedStatement.setNull(stmtIndex, Types.DATE);
                } else {
                    preparedStatement.setDate(stmtIndex, Date.valueOf((LocalDate) column[row]));
                }
                break;
            case ColumnType.TYPE_DATETIME:
                if (isNull) {
                    preparedStatement.setNull(stmtIndex, Types.TIMESTAMP);
                } else {
                    preparedStatement.setTimestamp(stmtIndex, Timestamp.valueOf((LocalDateTime) column[row]));
                }
                break;
            case ColumnType.TYPE_CHAR:
            case ColumnType.TYPE_VARCHAR:
                if (isNull) {
                    preparedStatement.setNull(stmtIndex, Types.VARCHAR);
                } else {
                    preparedStatement.setString(stmtIndex, (String) column[row]);
                }
                break;
            default:
                throw new RuntimeException("Unknown column type " + colType);
        }
    }
}
