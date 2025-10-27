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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/transaction/PublishVersionDaemon.java

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

package com.starrocks.transaction;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

// Publish Trace Info in each round
public class PublishTraceInfo {
    long publishTxnCount;
    LongAdder finishedTxnCount;
    LongAdder failedFinishedTxnCount;

    long totalCost;

    long createTaskCost;
    long submitTaskCost;
    long finishTaskCost;

    TraceElement collectTraceCost;

    // details trace in finish task process
    TraceElement checkAllTaskFinishCost;

    TraceElement checkTxnFinishCost;
    TraceElement tableLockCostInCheckTxnFinish;

    TraceElement finishTxnCost;
    TraceElement tableLockCostInTxnFinish;
    TraceElement txnDbLockCostInTxnFinish;
    TraceElement checkQuorumCost;
    TraceElement persistTxnStateCost;
    TraceElement updateCatalogCost;
    TraceElement listenerBusCost;

    public PublishTraceInfo() {
        finishedTxnCount = new LongAdder();
        failedFinishedTxnCount = new LongAdder();
        checkAllTaskFinishCost = new TraceElement();
        tableLockCostInCheckTxnFinish = new TraceElement();
        checkTxnFinishCost = new TraceElement();
        tableLockCostInTxnFinish = new TraceElement();
        txnDbLockCostInTxnFinish = new TraceElement();
        finishTxnCost = new TraceElement();
        collectTraceCost = new TraceElement();
        checkQuorumCost = new TraceElement();
        persistTxnStateCost = new TraceElement();
        updateCatalogCost = new TraceElement();
        listenerBusCost = new TraceElement();
    }

    public void reset() {
        publishTxnCount = 0;
        finishedTxnCount.reset();
        failedFinishedTxnCount.reset();

        totalCost = 0;
        createTaskCost = 0;
        submitTaskCost = 0;
        finishTaskCost = 0;

        collectTraceCost.reset();
        checkAllTaskFinishCost.reset();
        checkTxnFinishCost.reset();
        tableLockCostInCheckTxnFinish.reset();
        finishTxnCost.reset();
        tableLockCostInTxnFinish.reset();
        txnDbLockCostInTxnFinish.reset();
        checkQuorumCost.reset();
        persistTxnStateCost.reset();
        updateCatalogCost.reset();
        listenerBusCost.reset();
    }

    public void collectTxnTraceInfo(TransactionState txn) {
        long startTime = System.nanoTime();
        long txnID = txn.getTransactionId();
        checkAllTaskFinishCost.addVal(txn.getCheckAllPublishTaskFinishCost(), txnID);
        tableLockCostInCheckTxnFinish.addVal(txn.getTableLockCostInCanTxnFinish(), txnID);
        checkTxnFinishCost.addVal(txn.getCheckCanTxnFinishCost(), txnID);
        tableLockCostInTxnFinish.addVal(txn.getTableLockCostInTxnFinish(), txnID);
        txnDbLockCostInTxnFinish.addVal(txn.getTxnDBLockCostInTxnFinish(), txnID);
        finishTxnCost.addVal(txn.getFinishTxnCost(), txnID);
        checkQuorumCost.addVal(txn.getCheckQuorumCost(), txnID);
        persistTxnStateCost.addVal(txn.getPersistTxnStateCost(), txnID);
        updateCatalogCost.addVal(txn.getUpdateCatalogCost(), txnID);
        listenerBusCost.addVal(txn.getListenerBusCost(), txnID);
        collectTraceCost.addVal(System.nanoTime() - startTime, txnID);
    }

    static class TraceElement {
        LongAdder totalValue = new LongAdder();
        AtomicLong maxValue = new AtomicLong(Long.MIN_VALUE);
        AtomicLong txnID = new AtomicLong(-1);

        public void addVal(long val, long txnID) {
            totalValue.add(val);

            long prevMax;
            do {
                prevMax = maxValue.get();
                if (val <= prevMax) {
                    break;
                }
            } while (!maxValue.compareAndSet(prevMax, val));

            // update max txnId only when val is larger than prevMax
            if (val > prevMax) {
                this.txnID.set(txnID);
            }

        }

        public void reset() {
            totalValue.reset();
            maxValue.set(Long.MIN_VALUE);
            txnID.set(-1);
        }

        public long totalValue() {
            return totalValue.longValue();
        }

        public long maxValue() {
            return maxValue.longValue();
        }

        public long txnId() {
            return txnID.longValue();
        }
    }
}
