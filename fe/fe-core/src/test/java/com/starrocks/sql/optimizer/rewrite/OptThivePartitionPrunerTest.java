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

package com.starrocks.sql.optimizer.rewrite;

import org.junit.Assert;
import org.junit.Test;

import static com.starrocks.sql.optimizer.rewrite.OptThivePartitionPruner.checkPartValDateFormat;

public class OptThivePartitionPrunerTest {

    @Test
    public void testValid() {
        Assert.assertTrue(checkPartValDateFormat("2024"));
        Assert.assertTrue(checkPartValDateFormat("202402"));
        Assert.assertTrue(checkPartValDateFormat("20240229")); // leap year
        Assert.assertTrue(checkPartValDateFormat("2023122514"));
        Assert.assertTrue(checkPartValDateFormat("202312251430"));
    }

    @Test
    public void testInvalid() {
        Assert.assertFalse(checkPartValDateFormat(null));
        Assert.assertFalse(checkPartValDateFormat(""));
        Assert.assertFalse(checkPartValDateFormat("abc"));
        Assert.assertFalse(checkPartValDateFormat("202313"));
        Assert.assertFalse(checkPartValDateFormat("20230229")); // not leap year
        Assert.assertFalse(checkPartValDateFormat("2023122524"));  // hour error
        Assert.assertFalse(checkPartValDateFormat("202312251460")); // minute error
        Assert.assertFalse(checkPartValDateFormat("20231"));       // length error
    }

    @Test
    public void testBoundary() {
        Assert.assertFalse(checkPartValDateFormat("0000"));     // year=0
        Assert.assertTrue(checkPartValDateFormat("0001"));
        Assert.assertTrue(checkPartValDateFormat("000101"));
        Assert.assertTrue(checkPartValDateFormat("00010101"));
    }

}
