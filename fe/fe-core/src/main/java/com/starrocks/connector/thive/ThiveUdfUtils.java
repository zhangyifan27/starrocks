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

package com.starrocks.connector.thive;

import com.google.common.base.Strings;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TFunctionBinaryType;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Executors;

import static com.starrocks.connector.thive.ThiveFunctionRegistry.getFunctionInfo;
import static com.starrocks.connector.thive.TypeConvert.fromHiveTypeToStarRocks;
import static com.starrocks.utils.MD5Utils.computeChecksum;

public class ThiveUdfUtils {

    private static final Logger LOG = LogManager.getLogger(ThiveUdfUtils.class);

    public static final String THIVE_UDF_WRAPPER_CLASS = "com.tencent.starrocks.udf.ThiveUDFWrapper";

    public static String checksum;

    public static long functionId;

    public static void init() {
        //TODO: update this to avoid restart fe when thive udf jar changed
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                LOG.info("start to compute thive udf jar checksum");
                checksum = computeChecksum(Config.thive_udf_wrapper_url);
                // use now timestamp to generate thive udf function id at init
                functionId = System.currentTimeMillis();
            } catch (IOException | NoSuchAlgorithmException e) {
                throw new SemanticException("cannot to compute object's checksum", e);
            }
        });
    }

    /**
     * construct thive udf function
     */
    public static Function getThiveUdfFunction(FunctionName fnName, Type[] argTypes) {
        if (Strings.isNullOrEmpty(fnName.getFunction())) {
            throw new RuntimeException("function name [" + fnName.getFunction() + "] is null or empty");
        }
        if (!FeConstants.runningUnitTest) {
            // skip checking checksum when running ut
            if (Strings.isNullOrEmpty(checksum)) {
                throw new RuntimeException("checksum is null, thive udf jar is not ready");
            }
        }
        FunctionInfo functionInfo = getFunctionInfo(fnName.getFunction());
        if (functionInfo == null) {
            return null;
        }
        Type returnType = getResultType(functionInfo, argTypes);
        if (!fnName.isThiveFunction()) {
            fnName.setAsThiveFunction();
        }
        Function fn = ScalarFunction.createUdf(fnName, argTypes,
                returnType, false, TFunctionBinaryType.SRJAR,
                Config.thive_udf_wrapper_url, THIVE_UDF_WRAPPER_CLASS, "", "");
        fn.setChecksum(checksum);
        // set thive udf function id is negative to avoid conflicts with the builtin function
        fn.setFunctionId(-functionId);
        return fn;
    }

    /**
     * get function return type
     */
    public static Type getResultType(FunctionInfo functionInfo, Type[] argTypes) {
        try (GenericUDF udf = createGenericUDF(functionInfo.getDisplayName(), functionInfo.getFunctionClass())) {
            ObjectInspector[] thiveArgs = new ObjectInspector[argTypes.length];
            for (int i = 0; i < argTypes.length; i++) {
                thiveArgs[i] = TypeConvert.fromStarRocksToHiveType(argTypes[i]);
            }
            ObjectInspector objectInspector = udf.initialize(thiveArgs);
            return fromHiveTypeToStarRocks(objectInspector);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * convert udf to GenericUDF
     */
    public static GenericUDF createGenericUDF(String name, Class<?> cls) throws Exception {
        if (GenericUDF.class.isAssignableFrom(cls)) {
            Constructor<?> constructor = cls.getConstructor();
            return (GenericUDF) constructor.newInstance();
        } else if (UDF.class.isAssignableFrom(cls)) {
            return new GenericUDFBridge(name, false, cls.getName());
        }
        throw new RuntimeException(cls.getName() + "udf type is not supported");
    }

}