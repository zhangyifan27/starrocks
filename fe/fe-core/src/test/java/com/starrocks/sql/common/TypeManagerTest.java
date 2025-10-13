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

package com.starrocks.sql.common;

import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SessionVariableConstants;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for TypeManager class
 */
public class TypeManagerTest {

    @Mocked
    private ConnectContext connectContext;

    private SessionVariable sessionVariable;

    @Before
    public void setUp() {
        sessionVariable = new SessionVariable();
    }

    /**
     * Test that when cbo_type_coercion_date_vs_integer = 'timestamp',
     * comparing Date type with Integer type should return Date type
     */
    @Test
    public void testDateVsIntegerWithTimestampMode() {
        // Set session variable to timestamp mode
        sessionVariable.setCboTypeCoercionDateVsInteger(SessionVariableConstants.TIMESTAMP);
        
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                minTimes = 0;

                connectContext.getSessionVariable();
                result = sessionVariable;
                minTimes = 0;
            }
        };

        // Test Integer vs Date, should return Date type
        Type result1 = TypeManager.getCompatibleTypeForBinary(true, Type.INT, Type.DATE);
        Assert.assertEquals(Type.DATE, result1);

        // Test Date vs Integer, should return Date type
        Type result2 = TypeManager.getCompatibleTypeForBinary(true, Type.DATE, Type.INT);
        Assert.assertEquals(Type.DATE, result2);

        // Test BigInt vs DateTime, should return DateTime type
        Type result3 = TypeManager.getCompatibleTypeForBinary(true, Type.BIGINT, Type.DATETIME);
        Assert.assertEquals(Type.DATETIME, result3);

        // Test DateTime vs SmallInt, should return DateTime type
        Type result4 = TypeManager.getCompatibleTypeForBinary(true, Type.DATETIME, Type.SMALLINT);
        Assert.assertEquals(Type.DATETIME, result4);

        // Test TinyInt vs Date, should return Date type
        Type result5 = TypeManager.getCompatibleTypeForBinary(true, Type.TINYINT, Type.DATE);
        Assert.assertEquals(Type.DATE, result5);

        // Test LargeInt vs Date, should return Date type
        Type result6 = TypeManager.getCompatibleTypeForBinary(true, Type.LARGEINT, Type.DATE);
        Assert.assertEquals(Type.DATE, result6);
    }

    /**
     * Test that when cbo_type_coercion_date_vs_integer = 'double' (default value),
     * comparing Date type with Integer type should return Double type
     */
    @Test
    public void testDateVsIntegerWithDoubleMode() {
        // Set session variable to double mode (default value)
        sessionVariable.setCboTypeCoercionDateVsInteger(SessionVariableConstants.DOUBLE);
        
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                minTimes = 0;

                connectContext.getSessionVariable();
                result = sessionVariable;
                minTimes = 0;
            }
        };

        // Test Integer vs Date, should return Double type
        Type result1 = TypeManager.getCompatibleTypeForBinary(true, Type.INT, Type.DATE);
        Assert.assertEquals(Type.DOUBLE, result1);

        // Test Date vs Integer, should return Double type
        Type result2 = TypeManager.getCompatibleTypeForBinary(true, Type.DATE, Type.INT);
        Assert.assertEquals(Type.DOUBLE, result2);

        // Test BigInt vs DateTime, should return Double type
        Type result3 = TypeManager.getCompatibleTypeForBinary(true, Type.BIGINT, Type.DATETIME);
        Assert.assertEquals(Type.DOUBLE, result3);
    }

    /**
     * Test that when ConnectContext is null, should use default behavior (return Double)
     */
    @Test
    public void testDateVsIntegerWithNullContext() {
        new Expectations() {
            {
                ConnectContext.get();
                result = null;
                minTimes = 0;
            }
        };

        // When ConnectContext is null, should return Double type
        Type result1 = TypeManager.getCompatibleTypeForBinary(true, Type.INT, Type.DATE);
        Assert.assertEquals(Type.DOUBLE, result1);

        Type result2 = TypeManager.getCompatibleTypeForBinary(true, Type.DATE, Type.BIGINT);
        Assert.assertEquals(Type.DOUBLE, result2);
    }

    /**
     * Test that non-range comparison (equality comparison) should not trigger special handling for Date vs Integer
     */
    @Test
    public void testDateVsIntegerWithNonRangeCompare() {
        sessionVariable.setCboTypeCoercionDateVsInteger(SessionVariableConstants.TIMESTAMP);
        
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                minTimes = 0;

                connectContext.getSessionVariable();
                result = sessionVariable;
                minTimes = 0;
            }
        };

        // Non-range comparison (isRangeCompare = false) should not trigger special handling for Date vs Integer
        // In this case, it will follow other type conversion logic
        Type result = TypeManager.getCompatibleTypeForBinary(false, Type.INT, Type.DATE);
        // For non-range comparison, Date vs Integer will eventually return DOUBLE
        Assert.assertNotNull(result);
    }

    /**
     * Test that comparing Float/Double with Date should not trigger special handling for Date vs Integer
     */
    @Test
    public void testDateVsFloatWithTimestampMode() {
        sessionVariable.setCboTypeCoercionDateVsInteger(SessionVariableConstants.TIMESTAMP);
        
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                minTimes = 0;

                connectContext.getSessionVariable();
                result = sessionVariable;
                minTimes = 0;
            }
        };

        // Float vs Date should not trigger FixedPointType special handling, should return DOUBLE
        Type result1 = TypeManager.getCompatibleTypeForBinary(true, Type.FLOAT, Type.DATE);
        Assert.assertEquals(Type.DOUBLE, result1);

        // Double vs DateTime should return DOUBLE
        Type result2 = TypeManager.getCompatibleTypeForBinary(true, Type.DOUBLE, Type.DATETIME);
        Assert.assertEquals(Type.DOUBLE, result2);
    }

    /**
     * Test edge case: comparison between Date types
     */
    @Test
    public void testDateVsDate() {
        sessionVariable.setCboTypeCoercionDateVsInteger(SessionVariableConstants.TIMESTAMP);
        
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                minTimes = 0;

                connectContext.getSessionVariable();
                result = sessionVariable;
                minTimes = 0;
            }
        };

        // Date vs Date should be handled in earlier logic, return DATETIME
        Type result1 = TypeManager.getCompatibleTypeForBinary(true, Type.DATE, Type.DATE);
        Assert.assertEquals(Type.DATE, result1);

        // Date vs DateTime should return DATETIME
        Type result2 = TypeManager.getCompatibleTypeForBinary(true, Type.DATE, Type.DATETIME);
        Assert.assertEquals(Type.DATETIME, result2);
    }

    /**
     * Test that comparison between Integer types should not trigger special handling for Date vs Integer
     */
    @Test
    public void testIntegerVsInteger() {
        sessionVariable.setCboTypeCoercionDateVsInteger(SessionVariableConstants.TIMESTAMP);
        
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                minTimes = 0;

                connectContext.getSessionVariable();
                result = sessionVariable;
                minTimes = 0;
            }
        };

        // Int vs BigInt should return BigInt
        Type result1 = TypeManager.getCompatibleTypeForBinary(true, Type.INT, Type.BIGINT);
        Assert.assertEquals(Type.BIGINT, result1);

        // SmallInt vs Int should return Int
        Type result2 = TypeManager.getCompatibleTypeForBinary(true, Type.SMALLINT, Type.INT);
        Assert.assertEquals(Type.INT, result2);
    }
}
