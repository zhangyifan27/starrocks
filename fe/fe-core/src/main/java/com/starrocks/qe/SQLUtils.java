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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectProcessor.java

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

package com.starrocks.qe;

import com.google.common.base.Strings;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLUtils {
    private static final String SUPERSQL_TRACE_ID = "supersql_trace_id";

    public static String[] splitComment(String comment) {
        return comment.split("\\s+");
    }

    public static String getSupersqlTraceId(String comment) {
        String[] splitComments = splitComment(comment);
        for (String entry : splitComments) {
            String[] parts = entry.split("=");
            if (parts.length == 2 && parts[0].trim().equalsIgnoreCase(SUPERSQL_TRACE_ID)) {
                return parts[1].trim();
            }
        }
        return null;
    }

    public static String extractSupersqlTraceId(String sql) {
        if (!sql.contains("/*")) {
            return null;
        }
        // /\\*\\*?  /* or /**
        /* supersql_trace_id=abcd */ /*comment*/
        Pattern pattern = Pattern.compile("/\\*\\*?(.*?)\\*/", Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);

        while (matcher.find()) {
            String comment = matcher.group(1);
            String supersqlTraceId = getSupersqlTraceId(comment);
            if (!Strings.isNullOrEmpty(supersqlTraceId)) {
                return supersqlTraceId;
            }
        }
        return null;
    }

    public static String extractSupersqlTraceId(String sqlStatements, int index, int length) {
        String[] sqls = sqlStatements.split(";");
        if (sqls.length != length || index >= length || index < 0) {
            return null;
        }
        return extractSupersqlTraceId(sqls[index]);
    }
}
