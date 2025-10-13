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
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLUtils {
    private static final String SUPERSQL_TRACE_ID = "supersql_trace_id";
    private static final String FLOW_ID = "flow_id";

    public static String[] splitComment(String comment) {
        return comment.split("\\s+");
    }

    public static String getIdFromComment(String comment, String keyOfId, String delimiter) {
        String[] splitComments = splitComment(comment);
        for (String entry : splitComments) {
            String[] parts = entry.split(delimiter);
            if (parts.length == 2 && parts[0].trim().equalsIgnoreCase(keyOfId)) {
                return parts[1].trim();
            }
        }
        return null;
    }

    public static String extractIdFromComment(String sql, String keyOfId, String delimiter) {
        if (!sql.contains("/*")) {
            return null;
        }
        // /\\*\\*?  /* or /**
        /* supersql_trace_id=abcd */ /*comment*/
        Pattern pattern = Pattern.compile("/\\*\\*?(.*?)\\*/", Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);

        boolean extractFlowId = FLOW_ID.equals(keyOfId);
        while (matcher.find()) {
            String comment = matcher.group(1);
            String id = getIdFromComment(comment, keyOfId, delimiter);
            if (!Strings.isNullOrEmpty(id)) {
                return id;
            }

            if (extractFlowId) {
                // for json common like:  /*trace: {"trace_id":"abc123","extra_info":{"flow_id":"263245"}}*/
                id = extractFlowIdFromJsonComment(comment);
                if (!Strings.isNullOrEmpty(id)) {
                    return id;
                }
            }
        }
        return null;
    }

    private static String extractFlowIdFromJsonComment(String comment) {
        if (Strings.isNullOrEmpty(comment)) {
            return null;
        }
        try {
            String jsonStr = extractJsonFromComment(comment);
            if (jsonStr == null) {
                return null;
            }

            JSONObject jsonObject = new JSONObject(jsonStr);
            return findFlowIdRecursively(jsonObject);
        } catch (Exception ignored) {

        }

        return null;
    }

    private static String findFlowIdRecursively(Object jsonObject) {
        if (jsonObject == null) {
            return null;
        }

        if (jsonObject instanceof JSONObject) {
            JSONObject obj = (JSONObject) jsonObject;
            if (obj.has(FLOW_ID)) {
                Object value = obj.get(FLOW_ID);
                if (value != null) {
                    return value.toString();
                }
            }

            for (String key : obj.keySet()) {
                Object value = obj.get(key);
                String result = findFlowIdRecursively(value);
                if (result != null) {
                    return result;
                }
            }
        } else if (jsonObject instanceof JSONArray) {
            JSONArray array = (JSONArray) jsonObject;

            for (int i = 0; i < array.length(); i++) {
                Object value = array.get(i);
                String result = findFlowIdRecursively(value);
                if (result != null) {
                    return result;
                }
            }
        }

        return null;
    }

    private static String extractJsonFromComment(String comment) {
        if (Strings.isNullOrEmpty(comment)) {
            return null;
        }

        int jsonStart = comment.indexOf('{');
        int jsonEnd = comment.lastIndexOf('}');

        if (jsonStart >= 0 && jsonEnd > jsonStart) {
            return comment.substring(jsonStart, jsonEnd + 1);
        }

        return null;
    }

    public static String extractSupersqlTraceId(String sql) {
        return extractIdFromComment(sql, SUPERSQL_TRACE_ID, "=");
    }

    public static String extractFlowId(String sql) {
        return extractIdFromComment(sql, FLOW_ID, ":");
    }

    public static String extractSupersqlTraceId(String sqlStatements, int index, int length) {
        String[] sqls = sqlStatements.split(";");
        if (sqls.length != length || index >= length || index < 0) {
            return null;
        }
        return extractSupersqlTraceId(sqls[index]);
    }

    public static String extractFlowId(String sqlStatements, int index, int length) {
        String[] sqls = sqlStatements.split(";");
        if (sqls.length != length || index >= length || index < 0) {
            return null;
        }
        return extractFlowId(sqls[index]);
    }
}
