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
import com.starrocks.connector.parser.trino.FunctionCallTransformer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * This Class is the entry to to convert a Trino function to StarRocks built-in function, we use
 * {@link FunctionCallTransformer} to record the transformation of a function，Because a  function
 * name can take different arguments, a function with the same name can have multiple
 * FunctionCallTransformer, all stored in TRANSFORMER_MAP.
 */

public class Thive2SRFunctionCallTransformer {
    private static final Logger LOG = LogManager.getLogger(Thive2SRFunctionCallTransformer.class);

    // function name -> list of function transformer
    public static Properties TRANSFORMER_MAP = new Properties();
    public static String DEFAULT_DISABLE_FUNCTION_NAME = "__DISABLE__";

    public static String THIVE_FUNCTION_TRANSFORMER_2_SR_CONF = "/conf/thiveFunction2SR.conf";

    static {
        try {
            registerAllFunctionTransformer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to register all function transformers", e);
        }
    }

    private static void registerAllFunctionTransformer() throws IOException {
        // TODO: 从配置文件读取
        String starRocksHome = System.getenv("STARROCKS_HOME");
        try (FileReader reader = new FileReader(starRocksHome + THIVE_FUNCTION_TRANSFORMER_2_SR_CONF)) {
            TRANSFORMER_MAP.load(reader);
        } catch (Exception e) {
            LOG.error("Failed to load properties file", e);
            throw new IOException("Failed to load properties file", e);
        }
    }

    public static void reloadAllFunctionTransformer() throws IOException {
        // 加载到新的 Map
        String starRocksHome = System.getenv("STARROCKS_HOME");
        try (FileReader reader = new FileReader(starRocksHome + THIVE_FUNCTION_TRANSFORMER_2_SR_CONF)) {
            Properties props = new Properties();
            props.load(reader);
            TRANSFORMER_MAP = props;
        } catch (Exception e) {
            LOG.error("Failed to load properties file", e);
            throw new IOException("Failed to load properties file", e);
        }
    }

    public static String transformFunction(FunctionName functionName) throws RuntimeException {
        if (TRANSFORMER_MAP.containsKey(functionName.getFunction())) {
            String functionTransformerClassName = TRANSFORMER_MAP.get(functionName.getFunction()).toString();
            if (functionTransformerClassName.equals(DEFAULT_DISABLE_FUNCTION_NAME)) {
                throw new RuntimeException("Disabled function: " + functionName.getFunction());
            }
            if (functionTransformerClassName.startsWith(FunctionName.THIVE_UDF_DB)) {
                String[] parts = functionTransformerClassName.split("\\.", 2);
                if (parts.length != 2 || !parts[0].equals(FunctionName.THIVE_UDF_DB)) {
                    throw new RuntimeException("Invalid function transformer class name: " + functionTransformerClassName);
                }
                functionName.setFunction(parts[1]);
                functionName.setAsThiveFunction();
            } else {
                functionName.setFunction(functionTransformerClassName);
            }
            return functionTransformerClassName;
        }
        return null;
    }

    public static Properties getTransformerMap() {
        return TRANSFORMER_MAP;
    }

    public static String getTransformerFunctionName(String functionName) {
        if (TRANSFORMER_MAP.containsKey(functionName)) {
            return TRANSFORMER_MAP.get(functionName).toString();
        }
        return null;
    }
}
