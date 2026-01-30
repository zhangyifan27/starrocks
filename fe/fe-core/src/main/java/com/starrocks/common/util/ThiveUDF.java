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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class ThiveUDF {

    public static String dateSubEvaluate(String date, int days) {
        com.tencent.tdw_udf_cloud.hive.udf.UDFDateSub udfDateSub = new com.tencent.tdw_udf_cloud.hive.udf.UDFDateSub();
        return udfDateSub.evaluate(new Text(date), new IntWritable(days)).toString();
    }

    public static String dateAddEvaluate(String date, int days) {
        com.tencent.tdw_udf_cloud.hive.udf.UDFDateAdd udfDateAdd = new com.tencent.tdw_udf_cloud.hive.udf.UDFDateAdd();
        return udfDateAdd.evaluate(new Text(date), new IntWritable(days)).toString();
    }

    public static String addMonthsEvaluate(String date, int days) {
        com.tencent.tdw_udf_cloud.hive.udf.UDFADD_MONTHS udfAddMonths = new com.tencent.tdw_udf_cloud.hive.udf.UDFADD_MONTHS();
        return udfAddMonths.evaluate(new Text(date), new IntWritable(days)).toString();
    }

}
