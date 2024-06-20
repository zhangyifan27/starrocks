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

package com.starrocks.plugin;

import com.google.gson.annotations.SerializedName;

public class ProfileEvent {
    public enum EventType {
        QUERY,
        LOAD
    }

    @SerializedName("type")
    private EventType type;

    @SerializedName("queryId")
    private String queryId;

    @SerializedName("supersqlTraceId")
    private String supersqlTraceId;

    @SerializedName("profile")
    private String profile;

    public ProfileEvent() {
    }

    public ProfileEvent(EventType type, String queryId, String supersqlTraceId, String profile) {
        this.type = type;
        this.queryId = queryId;
        this.supersqlTraceId = supersqlTraceId;
        this.profile = profile;
    }

    public void setEventType(EventType type) {
        this.type = type;
    }

    public EventType getEventType() {
        return type;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setSupersqlTraceId(String supersqlTraceId) {
        this.supersqlTraceId = supersqlTraceId;
    }

    public String getSupersqlTraceId() {
        return supersqlTraceId;
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }
}
