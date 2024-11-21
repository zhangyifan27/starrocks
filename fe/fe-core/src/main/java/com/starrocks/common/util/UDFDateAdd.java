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

package com.starrocks.common.util;

import java.time.LocalDateTime;

public class UDFDateAdd {
    private static String FORMAT1 = "yyyy-MM-dd";
    private static String FORMAT2 = "yyyyMMdd";

    public static String evaluate(String inputStr, LocalDateTime dateTime) {
        if (inputStr.contains("-")) {
            //yyyy-MM-dd
            return dateTime.format(DateUtils.DATE_FORMATTER_UNIX);
        } else {
            //yyyyMMdd
            if (inputStr.trim().length() > 8) {
                return null;
            }
            return dateTime.format(DateUtils.DATEKEY_FORMATTER);
        }
    }

}
