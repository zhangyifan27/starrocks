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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
* https://git.woa.com/tdw/udf_cloud/blob/master/src/main/java/com/tencent/tdw_udf_cloud/hive/udf/UDFToChar.java
*/
public class UDFToChar {
    private static String FORMAT1 = "yyyymmdd";
    private static String FORMAT2 = "yyyymm";
    private static String FORMAT3 = "yyyy";
    private static String FORMAT4 = "mm";
    private static String FORMAT5 = "dd";
    private static String FORMAT6 = "yyyy-mm-dd";
    private static String FORMAT7 = "yyyy-mm";

    private static String FORMAT8 = "yyyymmddhh24miss";
    private static String FORMAT9 = "yyyy-mm-dd hh24:mi:ss";
    private static String FORMAT10 = "hh24miss";
    private static String FORMAT11 = "yyyymmddhh24missff3";

    public static String evaluate(String date, String format) {
        if (date == null) {
            return null;
        }

        if (format == null || format.length() == 0) {
            format = FORMAT1;
        }

        int year;
        int month;
        int day;
        int hour;
        int min;
        int second;
        int ff;
        if (format.equalsIgnoreCase(FORMAT1)) {
            Pattern pattern = Pattern.compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9])[\\s\\S]*(\\..*)?$");
            Matcher matcher = pattern.matcher(date);
            if (!matcher.matches()) {
                return null;
            }
            year = Integer.valueOf(matcher.group(1));
            month = Integer.valueOf(matcher.group(2));
            day = Integer.valueOf(matcher.group(3));
            return String.format("%04d%02d%02d", year, month, day);
        } else if (format.equalsIgnoreCase(FORMAT2)) {
            Pattern pattern = Pattern.compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])[\\s\\S]*(\\..*)?$");
            Matcher matcher = pattern.matcher(date);
            if (!matcher.matches()) {
                return null;
            }
            year = Integer.valueOf(matcher.group(1));
            month = Integer.valueOf(matcher.group(2));
            return String.format("%04d%02d", year, month);
        } else if (format.equalsIgnoreCase(FORMAT3)) {
            Pattern pattern = Pattern.compile("([0-9][0-9][0-9][0-9])[\\s\\S]*(\\..*)?$");
            Matcher matcher = pattern.matcher(date);
            if (!matcher.matches()) {
                return null;
            }
            year = Integer.valueOf(matcher.group(1));
            return String.format("%04d", year);
        } else if (format.equalsIgnoreCase(FORMAT4)) {
            Pattern pattern = Pattern.compile("[0-9][0-9][0-9][0-9]-([0-9][0-9])-[0-9][0-9][\\s\\S]*(\\..*)?$");
            Matcher matcher = pattern.matcher(date);
            if (!matcher.matches()) {
                return null;
            }
            month = Integer.valueOf(matcher.group(1));
            return String.format("%02d", month);
        } else if (format.equalsIgnoreCase(FORMAT5)) {
            Pattern pattern = Pattern.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-([0-9][0-9])[\\s\\S]*(\\..*)?$");
            Matcher matcher = pattern.matcher(date);
            if (!matcher.matches()) {
                return null;
            }
            day = Integer.valueOf(matcher.group(1));
            return String.format("%02d", day);
        } else if (format.equalsIgnoreCase(FORMAT6)) {
            Pattern pattern = Pattern.compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9])[\\s\\S]*(\\..*)?$");
            Matcher matcher = pattern.matcher(date);
            if (!matcher.matches()) {
                return null;
            }
            year = Integer.valueOf(matcher.group(1));
            month = Integer.valueOf(matcher.group(2));
            day = Integer.valueOf(matcher.group(3));
            return String.format("%04d-%02d-%02d", year, month, day);
        } else if (format.equalsIgnoreCase(FORMAT7)) {
            Pattern pattern = Pattern.compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])[\\s\\S]*(\\..*)?$");
            Matcher matcher = pattern.matcher(date);
            if (!matcher.matches()) {
                return null;
            }
            year = Integer.valueOf(matcher.group(1));
            month = Integer.valueOf(matcher.group(2));
            return String.format("%04d-%02d", year, month);
        } else if (format.equalsIgnoreCase(FORMAT8)) {
            Pattern pattern = Pattern.compile(
                    "([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9]) ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])[\\s\\S]*(\\..*)?$");
            Matcher matcher = pattern.matcher(date);
            if (!matcher.matches()) {
                return null;
            }
            year = Integer.valueOf(matcher.group(1));
            month = Integer.valueOf(matcher.group(2));
            day = Integer.valueOf(matcher.group(3));
            hour = Integer.valueOf(matcher.group(4));
            min = Integer.valueOf(matcher.group(5));
            second = Integer.valueOf(matcher.group(6));
            return String.format("%04d%02d%02d%02d%02d%02d", year, month, day, hour, min, second);
        } else if (format.equalsIgnoreCase(FORMAT9)) {
            Pattern pattern = Pattern.compile(
                    "([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9]) ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])[\\s\\S]*(\\..*)?$");
            Matcher matcher = pattern.matcher(date);
            if (!matcher.matches()) {
                return null;
            }
            year = Integer.valueOf(matcher.group(1));
            month = Integer.valueOf(matcher.group(2));
            day = Integer.valueOf(matcher.group(3));
            hour = Integer.valueOf(matcher.group(4));
            min = Integer.valueOf(matcher.group(5));
            second = Integer.valueOf(matcher.group(6));
            return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min, second);
        } else if (format.equalsIgnoreCase(FORMAT10)) {
            Pattern pattern = Pattern.compile(
                    "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])[\\s\\S]*(\\..*)?$");
            Matcher matcher = pattern.matcher(date);
            if (!matcher.matches()) {
                return null;
            }
            hour = Integer.valueOf(matcher.group(1));
            min = Integer.valueOf(matcher.group(2));
            second = Integer.valueOf(matcher.group(3));
            return String.format("%02d%02d%02d", hour, min, second);
        } else if (format.equalsIgnoreCase(FORMAT11)) {
            Pattern pattern = Pattern.compile(
                    "([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9]) ([0-9][0-9]):([0-9][0-9]):([0-9][0-9]):([0-9][0-9][0-9])" +
                            "[\\s\\S]*(\\..*)?$");
            Matcher matcher = pattern.matcher(date);
            if (!matcher.matches()) {
                return null;
            }
            year = Integer.valueOf(matcher.group(1));
            month = Integer.valueOf(matcher.group(2));
            day = Integer.valueOf(matcher.group(3));
            hour = Integer.valueOf(matcher.group(4));
            min = Integer.valueOf(matcher.group(5));
            second = Integer.valueOf(matcher.group(6));
            ff = Integer.valueOf(matcher.group(7));
            return String.format("%04d%02d%02d%02d%02d%02d%03d", year, month, day, hour, min, second, ff);
        } else {
            return null;
        }
    }

    public static String evaluate(LocalDateTime date, String format) {
        if (date == null) {
            return null;
        }

        if (format == null || format.length() == 0) {
            format = FORMAT1;
        }

        int year;
        int month;
        int day;
        int hour;
        int min;
        int second;
        int ff;
        if (format.equalsIgnoreCase(FORMAT1)) {
            year = date.getYear();
            month = date.getMonthValue();
            day = date.getDayOfMonth();
            return String.format("%04d%02d%02d", year, month, day);
        } else if (format.equalsIgnoreCase(FORMAT2)) {
            year = date.getYear();
            month = date.getMonthValue();
            return String.format("%04d%02d", year, month);
        } else if (format.equalsIgnoreCase(FORMAT3)) {
            year = date.getYear();
            return String.format("%04d", year);
        } else if (format.equalsIgnoreCase(FORMAT4)) {
            month = date.getMonthValue();
            return String.format("%02d", month);
        } else if (format.equalsIgnoreCase(FORMAT5)) {
            day = date.getDayOfMonth();
            return String.format("%02d", day);
        } else if (format.equalsIgnoreCase(FORMAT6)) {
            year = date.getYear();
            month = date.getMonthValue();
            day = date.getDayOfMonth();
            return String.format("%04d-%02d-%02d", year, month, day);
        } else if (format.equalsIgnoreCase(FORMAT7)) {
            year = date.getYear();
            month = date.getMonthValue();
            return String.format("%04d-%02d", year, month);
        } else if (format.equalsIgnoreCase(FORMAT8)) {
            year = date.getYear();
            month = date.getMonthValue();
            day = date.getDayOfMonth();
            hour = date.getHour();
            min = date.getMinute();
            second = date.getSecond();
            return String.format("%04d%02d%02d%02d%02d%02d", year, month, day, hour, min, second);
        } else if (format.equalsIgnoreCase(FORMAT9)) {
            year = date.getYear();
            month = date.getMonthValue();
            day = date.getDayOfMonth();
            hour = date.getHour();
            min = date.getMinute();
            second = date.getSecond();
            return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min, second);
        } else if (format.equalsIgnoreCase(FORMAT10)) {
            hour = date.getHour();
            min = date.getMinute();
            second = date.getSecond();
            return String.format("%02d%02d%02d", hour, min, second);
        } else if (format.equalsIgnoreCase(FORMAT11)) {
            year = date.getYear();
            month = date.getMonthValue();
            day = date.getDayOfMonth();
            hour = date.getHour();
            min = date.getMinute();
            second = date.getSecond();
            ff = date.getNano() / 1000;
            return String.format("%04d%02d%02d%02d%02d%02d%03d", year, month, day, hour, min, second, ff);
        } else {
            return null;
        }
    }
}
