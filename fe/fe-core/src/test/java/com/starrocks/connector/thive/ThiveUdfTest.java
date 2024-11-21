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

package com.starrocks.connector.thive;

import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import org.junit.Assert;
import org.junit.Test;

import static com.starrocks.connector.thive.ThiveFunctionRegistry.getFunctionInfo;
import static com.starrocks.connector.thive.ThiveUdfUtils.getResultType;

public class ThiveUdfTest {

    @Test
    public void testUdfReturnType() {
        Type returnType;

        returnType = getResultType(getFunctionInfo("abs"), new Type[] {ScalarType.INT});
        Assert.assertEquals(ScalarType.INT, returnType);

        returnType = getResultType(getFunctionInfo("sccdateadd"), new Type[] {ScalarType.VARCHAR, ScalarType.INT});
        Assert.assertEquals(ScalarType.createDefaultCatalogString(), returnType);

        returnType = getResultType(getFunctionInfo("rand"), new Type[] {ScalarType.INT});
        Assert.assertEquals(ScalarType.DOUBLE, returnType);
    }

    @Test
    public void getThiveUdfFunction() {
        FeConstants.runningUnitTest = true;
        FunctionName functionName = new FunctionName("hive_base64");
        Function function = ThiveUdfUtils.getThiveUdfFunction(functionName, new Type[] {ScalarType.VARCHAR});
        Assert.assertTrue(function instanceof ScalarFunction);
        ScalarFunction scalarFunction = (ScalarFunction) function;
        Assert.assertEquals("com.tencent.starrocks.udf.ThiveUDFWrapper", scalarFunction.getSymbolName());
        Assert.assertEquals(true, functionName.isThiveFunction());
        Assert.assertEquals(ScalarType.createDefaultCatalogString(), scalarFunction.getReturnType());
    }
}
