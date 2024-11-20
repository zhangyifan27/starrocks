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

#include "common/logging.h"
#include "fs/hdfs/tauth_env.h"

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <mutex>

#include "exprs/base64.h"

namespace starrocks {

TauthEnv& TauthEnv::Instance() {
    static TauthEnv global_instance;
    static std::once_flag initFlag;
    std::call_once(initFlag, [&]() {
        global_instance.init();
    });
    return global_instance;
}

void TauthEnv::init() {
    tauthEnvSet = true;
    tq_platform_user_name = getenv("TQ_PLATFORM_USER_NAME");
    if (tq_platform_user_name == nullptr || strlen(tq_platform_user_name) <= 0) {
        LOG(INFO) << "TQ_PLATFORM_USER_NAME not set";
        tauthEnvSet = false;
    }
    tdw_pri_user_name = getenv("TDW_PRI_USER_NAME");
    if (tdw_pri_user_name == nullptr || strlen(tdw_pri_user_name) <= 0) {
        LOG(INFO) << "TDW_PRI_USER_NAME not set";
        tauthEnvSet = false;
    }

    tq_platform_user_cmk = std::getenv("TQ_PLATFORM_USER_CMK");
    if (tq_platform_user_cmk == nullptr || strlen(tq_platform_user_cmk) <= 0) {
        LOG(INFO) << "TQ_PLATFORM_USER_CMK not set";
        tauthEnvSet = false;
    } else {
        size_t cmk_length = strlen(tq_platform_user_cmk);
        size_t encoded_length = (size_t)(4.0 * ceil((double)cmk_length / 3.0));
        unsigned char* encoded_data = new unsigned char[encoded_length + 1]; // +1 for the null-terminator
        encoded_data[encoded_length] = '\0'; // Null-terminate the string
        base64_encode2((unsigned char*)tq_platform_user_cmk, cmk_length, encoded_data);
        tq_platform_user_cmk_base64 = reinterpret_cast<const char*>(encoded_data);
    }
}

const char* TauthEnv::getTqPlatformUserName() {
    return tq_platform_user_name;
}

const char* TauthEnv::getTqPlatformUserCmk() {
    return tq_platform_user_cmk;
}

const char* TauthEnv::getTdwPriUserName() {
    return tdw_pri_user_name;
}

const bool TauthEnv::isTauthEnvSet() {
    return tauthEnvSet;
}

void TauthEnv::setTauthConf(struct hdfsBuilder *hdfs_builder, const std::string& hdfs_username) {
    if (!tauthEnvSet) {
        return;
    }
    hdfsBuilderConfSetStr(hdfs_builder, "hadoop.user.group.support.env.token",
                          "false");
    hdfsBuilderConfSetStr(hdfs_builder, "hadoop.libhdfs.cmk.conf.enable",
                          "true");
    hdfsBuilderConfSetStr(hdfs_builder, "hadoop.libhdfs.cmk.user",
                          tq_platform_user_name);
    hdfsBuilderConfSetStr(hdfs_builder, "hadoop.libhdfs.cmk.key",
                          tq_platform_user_cmk_base64);
    hdfsBuilderConfSetStr(hdfs_builder, "hadoop.libhdfs.cmk.epoch", "-1");
    if (hdfs_username.empty()) {
        hdfsBuilderConfSetStr(hdfs_builder, "hadoop.libhdfs.cmk.proxy.user",
                              tdw_pri_user_name);
    } else {
        hdfsBuilderConfSetStr(hdfs_builder, "hadoop.libhdfs.cmk.proxy.user",
                              hdfs_username.data());
    }
}

} // namespace starrocks
