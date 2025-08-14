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

/**
 * https://git.woa.com/tdw/udf_cloud/blob/master/src/main/java/com/tencent/tdw_udf_cloud/hive/udf/UDFToDate2.java
 */
public class UDFToDate2 {

    public static String evaluate(String date, String format) throws AnalysisException {
        try {
            com.tencent.tdw_udf_cloud.hive.udf.UDFToDate2 udfToDate =
                    new com.tencent.tdw_udf_cloud.hive.udf.UDFToDate2();
            return udfToDate.evaluate(date, format);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    public static String evaluate(String date) throws AnalysisException {
        try {
            com.tencent.tdw_udf_cloud.hive.udf.UDFToDate2 udfToDate =
                    new com.tencent.tdw_udf_cloud.hive.udf.UDFToDate2();
            return udfToDate.evaluate(date);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    public static String evaluate(LocalDateTime date) {
        if (date == null) {
            return null;
        }
        int year = date.getYear();
        int month = date.getMonthValue();
        int day = date.getDayOfMonth();
        int hour = date.getHour();
        int min = date.getMinute();
        int second = date.getSecond();
        int ff = date.getNano() / 1000;

        return String.format("%04d-%02d-%02d %02d:%02d:%02d:%03d", year, month, day, hour, min, second, ff);
    }

}
