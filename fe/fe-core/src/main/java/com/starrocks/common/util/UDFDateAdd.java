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

import com.starrocks.common.AnalysisException;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;

public class UDFDateAdd {
    private static String FORMAT1 = "yyyy-MM-dd";
    private static String FORMAT2 = "yyyyMMdd";
    public static final String MILLISECOND = "millisecond";
    public static final String SECOND = "second";
    public static final String MINUTE = "minute";
    public static final String HOUR = "hour";
    public static final String DAY = "day";
    public static final String WEEK = "week";
    public static final String MONTH = "month";
    public static final String QUARTER = "quarter";
    public static final String YEAR = "year";

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

    public static LocalDateTime plus(String type, long plus, LocalDateTime dateTime) throws AnalysisException {
        if (type == null) {
            throw new AnalysisException("type cannot be null");
        }
        if (type.equalsIgnoreCase(MILLISECOND)) {
            return dateTime.plus(plus, ChronoUnit.MILLIS);
        } else if (type.equalsIgnoreCase(SECOND)) {
            return dateTime.plusSeconds(plus);
        } else if (type.equalsIgnoreCase(MINUTE)) {
            return dateTime.plusMinutes(plus);
        } else if (type.equalsIgnoreCase(HOUR)) {
            return dateTime.plusHours(plus);
        } else if (type.equalsIgnoreCase(DAY)) {
            return dateTime.plusDays(plus);
        } else if (type.equalsIgnoreCase(WEEK)) {
            return dateTime.plusWeeks(plus);
        } else if (type.equalsIgnoreCase(MONTH)) {
            return dateTime.plusMonths(plus);
        } else if (type.equalsIgnoreCase(QUARTER)) {
            return dateTime.plus(plus, IsoFields.QUARTER_YEARS);
        } else if (type.equalsIgnoreCase(YEAR)) {
            return dateTime.plusYears(plus);
        } else {
            throw new AnalysisException("tdw_date_add unsupported plus type " + type);
        }
    }

}
