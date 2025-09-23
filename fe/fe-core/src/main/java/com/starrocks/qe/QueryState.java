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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/QueryState.java

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

package com.starrocks.qe;

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.ErrorCode;
import com.starrocks.mysql.MysqlEofPacket;
import com.starrocks.mysql.MysqlErrPacket;
import com.starrocks.mysql.MysqlOkPacket;
import com.starrocks.mysql.MysqlPacket;
import com.starrocks.thrift.TStatusCode;

import java.util.LinkedHashMap;
import java.util.Map;

// query state used to record state of query, maybe query status is better
public class QueryState {
    public enum MysqlStateType {
        NOOP,   // send nothing to remote
        OK,     // send OK packet to remote
        EOF,    // send EOF packet to remote
        ERR;     // send ERROR packet to remote

        private static final ImmutableMap<String, MysqlStateType> STATES =
                new ImmutableMap.Builder<String, MysqlStateType>()
                        .put("NOOP", NOOP)
                        .put("OK", OK)
                        .put("EOF", EOF)
                        .put("ERR", ERR)
                        .build();

        public static MysqlStateType fromString(String state) {
            return STATES.get(state);
        }
    }

    public enum ErrType {
        ANALYSIS_ERR,
        IGNORE_ERR,

        INTERNAL_ERR,

        IO_ERR,

        // execution Timeout
        EXEC_TIME_OUT,

        UNKNOWN
    }

    public enum RequestType {
        SELECT,
        INSERT,
        DELETE,
        UPDATE,
        SET,
        UNKNOWN
    }

    private MysqlStateType stateType = MysqlStateType.OK;
    private ErrorCode errorCode;
    private String infoMessage;
    private boolean isQuery = false;
    private RequestType requestType = RequestType.UNKNOWN;
    private long affectedRows = 0;
    private int warningRows = 0;
    private boolean isHotColdQuery = false;
    // make it public for easy to use
    public int serverStatus = 0;
    private boolean isFinished = false;

    private ErrType errType = ErrType.UNKNOWN;
    private String errorMessage = "";

    // Map of Query error type and error message, used for audit log.
    // Here, we use LinkedHashMap to record the first errType and errorMessage because
    // they are often the root cause of a failed query. Considering some corner cases,
    // we also record the remaining errorTypes and errorMessages to avoid overwriting the root cause.
    private final Map<String, String> errorMaps = new LinkedHashMap<>(); // errType -> errorMessage

    public QueryState() {
    }

    public void reset() {
        stateType = MysqlStateType.OK;
        errorMessage = "";
        errorCode = null;
        infoMessage = null;
        errType = ErrType.UNKNOWN;
        isQuery = false;
        affectedRows = 0;
        warningRows = 0;
        serverStatus = 0;
        isFinished = false;
        requestType = RequestType.UNKNOWN;
        errorMaps.clear();
    }

    public MysqlStateType getStateType() {
        return stateType;
    }

    public void setEof() {
        stateType = MysqlStateType.EOF;
        isFinished = true;
    }

    public void setOk() {
        setOk(0, 0, null);
    }

    public void setOk(long affectedRows, int warningRows, String infoMessage) {
        this.affectedRows = affectedRows;
        this.warningRows = warningRows;
        this.infoMessage = infoMessage;
        stateType = MysqlStateType.OK;
        isFinished = true;
    }

    public void setError(String errorMsg) {
        this.stateType = MysqlStateType.ERR;
        this.setMsg(errorMsg);
        isFinished = true;
    }

    public boolean isError() {
        return stateType == MysqlStateType.ERR;
    }

    public boolean isRunning() {
        return !isFinished;
    }

    public void setStateType(MysqlStateType stateType) {
        this.stateType = stateType;
    }

    public void setMsg(String msg) {
        this.errorMessage = msg == null ? "" : msg;
        addToErrorMaps(ErrType.UNKNOWN.name(), this.errorMessage);
    }

    public void resetErrTypeAndMsg() {
        this.errorMessage = "";
        this.errType = ErrType.UNKNOWN;
        errorMaps.clear();
    }

    public void setErrTypeAndMsg(ErrType errType, String errMsg) {
        this.errType = errType == null ? ErrType.UNKNOWN : errType;
        this.stateType = MysqlStateType.ERR;
        this.errorMessage = errMsg == null ? "" : errMsg;
        isFinished = true;

        addToErrorMaps(this.errType.name(), this.errorMessage);
    }

    public void setErrStatusCodeAndMsg(TStatusCode errCode, String errMsg) {
        this.stateType = MysqlStateType.ERR;
        this.errorMessage = errMsg == null ? "" : errMsg;
        isFinished = true;

        addToErrorMaps(errCode == null ? "UNKNOWN" : errCode.toString(), this.errorMessage);
    }

    private void addToErrorMaps(String errorCode, String errorMessage) {
        if (errorCode.equals(ErrType.UNKNOWN.name()) && errorMessage.isEmpty()) {
            return;
        }
        if (!errorMaps.containsKey(errorCode)) {
            errorMaps.put(errorCode, errorMessage);
        }
    }

    public String getRootErrorCode() {
        if (!errorMaps.isEmpty()) {
            return errorMaps.entrySet().iterator().next().getKey();
        }
        return "";
    }

    public String getRootErrorMessage() {
        if (!errorMaps.isEmpty()) {
            return errorMaps.entrySet().iterator().next().getValue();
        }
        return "";
    }

    public String printErrorCodesAndMsgs() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : errorMaps.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            sb.append("\"").append(entry.getKey()).append("\":\"").append(entry.getValue()).append("\"");
            first = false;
        }
        return sb.toString();
    }

    public ErrType getErrType() {
        return errType;
    }

    public void setIsQuery(boolean isQuery) {
        this.isQuery = isQuery;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public void setRequestType(RequestType requestType) {
        this.requestType = requestType;
    }

    public boolean isQuery() {
        return isQuery;
    }

    public String getInfoMessage() {
        return infoMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(ErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public int getWarningRows() {
        return warningRows;
    }

    public void setWarningRows(int warningRows) {
        this.warningRows = warningRows;
    }

    public MysqlPacket toResponsePacket() {
        MysqlPacket packet = null;
        switch (stateType) {
            case OK:
                packet = new MysqlOkPacket(this);
                break;
            case EOF:
                packet = new MysqlEofPacket(this);
                break;
            case ERR:
                packet = new MysqlErrPacket(this);
                break;
            default:
                break;
        }
        return packet;
    }

    public String toProfileString() {
        if (stateType == MysqlStateType.ERR) {
            return "Error";
        } else if (isFinished) {
            return "Finished";
        } else {
            return "Running";
        }
    }

    @Override
    public String toString() {
        return String.valueOf(stateType);
    }
}
