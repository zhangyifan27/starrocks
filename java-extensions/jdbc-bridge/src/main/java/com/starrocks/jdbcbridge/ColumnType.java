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

public class ColumnType {
    public static final int TYPE_UNKNOWN = 0;
    public static final int TYPE_TINYINT = 1; // MYSQL_TYPE_TINY
    public static final int TYPE_UNSIGNED_TINYINT = 2;
    public static final int TYPE_SMALLINT = 3; // MYSQL_TYPE_SHORT
    public static final int TYPE_UNSIGNED_SMALLINT = 4;
    public static final int TYPE_INT = 5; // MYSQL_TYPE_LONG
    public static final int TYPE_UNSIGNED_INT = 6;
    public static final int TYPE_BIGINT = 7; // MYSQL_TYPE_LONGLONG
    public static final int TYPE_UNSIGNED_BIGINT = 8;
    public static final int TYPE_LARGEINT = 9;
    public static final int TYPE_FLOAT = 10;  // MYSQL_TYPE_FLOAT
    public static final int TYPE_DOUBLE = 11; // MYSQL_TYPE_DOUBLE
    public static final int TYPE_DISCRETE_DOUBLE = 12;
    public static final int TYPE_CHAR = 13;        // MYSQL_TYPE_STRING
    public static final int TYPE_DATE_V1 = 14;     // MySQL_TYPE_NEWDATE
    public static final int TYPE_DATETIME_V1 = 15; // MySQL_TYPE_DATETIME
    public static final int TYPE_DECIMAL = 16;     // DECIMAL; using different store format against MySQL
    public static final int TYPE_VARCHAR = 17;

    public static final int TYPE_STRUCT = 18; // Struct
    public static final int TYPE_ARRAY = 19;  // ARRAY
    public static final int TYPE_MAP = 20;    // Map
    public static final int TYPE_NONE = 22;
    public static final int TYPE_HLL = 23;
    public static final int TYPE_BOOLEAN = 24;
    public static final int TYPE_OBJECT = 25;

    // Added by StarRocks
    // Reserved some field for commutiy version

    public static final int TYPE_NULL = 42;
    public static final int TYPE_FUNCTION = 43;
    public static final int TYPE_TIME = 44;
    public static final int TYPE_BINARY = 45;
    public static final int TYPE_VARBINARY = 46;
    // decimal v3 type
    public static final int TYPE_DECIMAL32 = 47;
    public static final int TYPE_DECIMAL64 = 48;
    public static final int TYPE_DECIMAL128 = 49;
    public static final int TYPE_DATE = 50;
    public static final int TYPE_DATETIME = 51;
    public static final int TYPE_DECIMALV2 = 52;
    public static final int TYPE_PERCENTILE = 53;

    public static final int TYPE_JSON = 54;

    // max value of LogicalType; newly-added type should not exceed this value.
    // used to create a fixed-size hash map.
    public static final int TYPE_MAX_VALUE = 55;
}
