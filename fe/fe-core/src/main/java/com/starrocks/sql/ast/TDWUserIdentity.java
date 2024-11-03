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
package com.starrocks.sql.ast;

import com.starrocks.thrift.TUserIdentity;
import com.starrocks.utils.TdwRestClient;
import com.starrocks.utils.TdwUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by andrewcheng on 2024/3/20.
 */
public class TDWUserIdentity extends UserIdentity {
    private final Set<String> userGroup;

    private String realUser;

    public TDWUserIdentity(UserIdentity userIdentity) {
        this(userIdentity, userIdentity.getUser());
    }

    public TDWUserIdentity(UserIdentity userIdentity, String realUser) {
        super(userIdentity.getUser(), userIdentity.getHost(), userIdentity.isDomain());
        if (this.getUser() != null) {
            this.userGroup = TdwRestClient.getInstance().getUserGroup(TdwUtil.getTdwUserName(userIdentity.getUser()));
        } else {
            userGroup = new HashSet<>();
        }
        this.realUser = realUser;
    }

    public Set<String> getUserGroup() {
        return userGroup;
    }

    public String getRealUser() {
        return realUser;
    }

    @Override
    public TUserIdentity toThrift() {
        TUserIdentity tUserIdent = super.toThrift();
        tUserIdent.setReal_user(realUser);
        return tUserIdent;
    }
}
