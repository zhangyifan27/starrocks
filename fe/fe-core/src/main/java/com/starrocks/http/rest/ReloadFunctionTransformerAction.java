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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/RestBaseAction.java

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

package com.starrocks.http.rest;

import com.starrocks.connector.thive.Thive2SRFunctionCallTransformer;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import io.netty.handler.codec.http.HttpMethod;

import java.io.IOException;

//fehost:port/api/reload_function_transformer
public class ReloadFunctionTransformerAction extends RestBaseAction {
    public ReloadFunctionTransformerAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/reload_function_transformer",
                new ReloadFunctionTransformerAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        response.setContentType("application/json");
        RestResult result = new RestResult();

        try {
            Thive2SRFunctionCallTransformer.reloadAllFunctionTransformer();
            result.addResultEntry("Message",
                    "[Succeed] The function converter has been reloaded. If there are any modifications to the " +
                            "mapping configuration, please ensure that all front-end nodes are updated accordingly. ");
        } catch (IOException e) {
            result.addResultEntry("Message", e.getMessage());
        }

        sendResult(request, response, result);
    }
}

