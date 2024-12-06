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

import com.starrocks.journal.JournalTask;
import com.starrocks.tqmeta.replay.ReplayData;

import java.util.List;

public class EditLogEvent {
    public enum EventType {
        START,
        NORMAL,
        DISCARD,
        DATA,
        REPLAY
    }

    private EditLogEvent.EventType type;

    private List<JournalTask> journalTasks;
    private ReplayData replayData;
    private long journalId;
    private long discard;

    public EditLogEvent() {
    }

    public EditLogEvent(List<JournalTask> journalTasks, long journalId) {
        this.type = EventType.NORMAL;
        this.journalTasks = journalTasks;
        this.journalId = journalId;
    }

    public EditLogEvent(ReplayData data, long journalId) {
        this.type = EventType.REPLAY;
        this.replayData = data;
        this.journalId = journalId;
    }

    public EditLogEvent(long discard) {
        this.type = EventType.DISCARD;
        this.discard = discard;
    }

    public void setEventType(EditLogEvent.EventType type) {
        this.type = type;
    }

    public EditLogEvent.EventType getEventType() {
        return type;
    }

    public void setJournalTasks(List<JournalTask> journalTasks) {
        this.journalTasks = journalTasks;
    }

    public List<JournalTask> getJournalTasks() {
        return journalTasks;
    }

    public void setReplayData(ReplayData replayData) {
        this.replayData = replayData;
    }

    public ReplayData getReplayData() {
        return replayData;
    }

    public void setJournalId(long journalId) {
        this.journalId = journalId;
    }

    public long getJournalId() {
        return journalId;
    }
}
