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

import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.PublishVersionTask;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class is used to check and finish transactions that have already been published.
 */
public class PublishChecker {
    private static final Logger LOG = LogManager.getLogger(PublishChecker.class);

    private final PublishTraceInfo traceInfo;

    private volatile boolean isRunning;


    // sorted by commitedTime
    private final BlockingQueue<TransactionState> publishingTxnQueue = new LinkedBlockingQueue<>();

    public PublishChecker(PublishTraceInfo traceInfo) {
        this.traceInfo = traceInfo;
        this.isRunning = true;
    }



    public void addTxnToQueue(TransactionState transactionState) throws InterruptedException {
        publishingTxnQueue.put(transactionState);
    }

    public void checkAndFinishTxns() throws InterruptedException, UserException {
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        while (isRunning) {
            if (Thread.currentThread().isInterrupted()) {
                break;
            }

            // blocking get txn state
            TransactionState transactionState = publishingTxnQueue.take();

            long checkInterval = System.currentTimeMillis() - transactionState.getLastCheckPublishTimeMs();
            if (checkInterval < Config.publish_check_interval_ms) {
                // sleep to avoid checking too frequently
                Thread.sleep(Config.publish_check_interval_ms - checkInterval);
            }
            transactionState.setLastCheckPublishTimeMs(System.currentTimeMillis());

            checkAndFinishOneTxn(globalTransactionMgr, transactionState);

            if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
                // txn has not finished publishing yet
                publishingTxnQueue.put(transactionState);
            }
        }
    }

    public void checkAndFinishOneTxn(GlobalTransactionMgr globalTransactionMgr,
                                     TransactionState transactionState) throws UserException {
        // try to finish the transaction, if failed just retry in next loop
        transactionState.increaseCheckCount();
        Map<Long, PublishVersionTask> transTasks = transactionState.getPublishVersionTasks();
        Set<Long> publishErrorReplicaIds = Sets.newHashSet();
        Set<Long> unfinishedBackends = Sets.newHashSet();
        boolean allTaskFinished = true;
        long checkAllFinishedTime = System.nanoTime();
        for (PublishVersionTask publishVersionTask : transTasks.values()) {
            if (publishVersionTask.isFinished()) {
                // sometimes backend finish publish version task, but it maybe failed to change
                // transaction id to version for some tablets,
                // and it will upload the failed tablet info to fe and fe will deal with them
                Set<Long> errReplicas = publishVersionTask.getErrorReplicas();
                if (!errReplicas.isEmpty()) {
                    publishErrorReplicaIds.addAll(errReplicas);
                }
            } else {
                allTaskFinished = false;
                // Publish version task may succeed and finish in quorum replicas
                // but not finish in one replica.
                // here collect the backendId that do not finish publish version
                unfinishedBackends.add(publishVersionTask.getBackendId());
            }
        }

        long checkCanFinishTime = System.nanoTime();
        transactionState.addCheckAllPublishTaskFinishCost(checkCanFinishTime - checkAllFinishedTime);
        boolean shouldFinishTxn = true;
        if (!allTaskFinished) {
            shouldFinishTxn = globalTransactionMgr.canTxnFinished(transactionState,
                    publishErrorReplicaIds, unfinishedBackends);
            transactionState.addCheckCanTxnFinishCost(System.nanoTime() - checkCanFinishTime);
        }

        if (shouldFinishTxn) {
            traceInfo.finishedTxnCount.add(1);
            globalTransactionMgr.finishTransaction(transactionState.getDbId(), transactionState.getTransactionId(),
                    publishErrorReplicaIds);
            if (transactionState.getTransactionStatus() != TransactionStatus.VISIBLE) {
                transactionState.updateSendTaskTime();
                traceInfo.failedFinishedTxnCount.add(1);
                LOG.debug("publish version for transaction {} failed, has {} error replicas during publish",
                        transactionState, publishErrorReplicaIds.size());
            } else {
                for (PublishVersionTask task : transactionState.getPublishVersionTasks().values()) {
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUBLISH_VERSION, task.getSignature());
                }
                // clear publish version tasks to reduce memory usage when state changed to visible.
                transactionState.clearAfterPublished();
            }
        }

        // collect one txn trace info
        traceInfo.collectTxnTraceInfo(transactionState);
    }

    public void stop() {
        this.isRunning = false;
    }
}
