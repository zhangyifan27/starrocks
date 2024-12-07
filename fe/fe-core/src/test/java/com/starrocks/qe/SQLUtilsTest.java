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

package com.starrocks.qe;

import org.junit.Assert;
import org.junit.Test;

public class SQLUtilsTest {

    @Test
    public void testExtractSupersqlTraceId() {
        String sql = "select * from test";
        String supersqlTraceId = SQLUtils.extractSupersqlTraceId(sql);
        Assert.assertEquals(null, supersqlTraceId);

        sql = "/* supersql_trace_id=abcd *//* mixquery trace_id:pe1l3788qhjp_department_list query_unique_key:simple_STARROCKS_3307625697006734393 biz_id:gongfeng_tencent user_name:hanleyzhao panel_id:249031 card_id:department_list c_caller:DATATALK c_biz_id:gongfeng model_type:sql request_type:0 c_param_id:4ce5e5f5b1124a2a4944c3f893a9c0c7 c_scene_type:dashboard c_query_type:variable is_force_refresh:FORCE*/ select * from test";
        supersqlTraceId = SQLUtils.extractSupersqlTraceId(sql);
        Assert.assertEquals("abcd", supersqlTraceId);

        sql = "/** supersql_trace_id=295c2889-80d6-48fa-bc94-77786a94279d::0::18::s0\n" +
                " */\n" +
                "SELECT `cstring`, `cint`, COUNT(*) AS `EXPR_2`\n" +
                "FROM `test`.`supersql_temp`\n" +
                "GROUP BY `cint`, `cstring`";
        supersqlTraceId = SQLUtils.extractSupersqlTraceId(sql);
        Assert.assertEquals("295c2889-80d6-48fa-bc94-77786a94279d::0::18::s0", supersqlTraceId);

        sql = "/* supersql_trace_id=abcd *//* mixquery trace_id:pe1l3788qhjp_department_list query_unique_key:simple_STARROCKS_3307625697006734393 biz_id:gongfeng_tencent user_name:hanleyzhao panel_id:249031*/ select * from test; select * from test /* comment */";

        supersqlTraceId = SQLUtils.extractSupersqlTraceId(sql, 0, 2);
        Assert.assertEquals("abcd", supersqlTraceId);

        supersqlTraceId = SQLUtils.extractSupersqlTraceId(sql, 1, 2);
        Assert.assertEquals(null, supersqlTraceId);
    }

}
