/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider.ResponseHandle;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.types.Either;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

/**
 * Handles the response of {@link PartitionProducerStateProvider}.
 *
 * <p>The method {@code isProducerReadyOrAbortConsumption} determines whether the partition producer
 * is in a producing state, ready for consumption. Otherwise it aborts the consumption.
 */
public class RemoteChannelStateChecker {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteChannelStateChecker.class);

    private final ResultPartitionID resultPartitionId;

    private final String taskNameWithSubtask;

    public RemoteChannelStateChecker(
            ResultPartitionID resultPartitionId, String taskNameWithSubtask) {
        this.resultPartitionId = resultPartitionId;
        this.taskNameWithSubtask = taskNameWithSubtask;
    }

    public boolean isProducerReadyOrAbortConsumption(ResponseHandle responseHandle) {
        Either<ExecutionState, Throwable> result = responseHandle.getProducerExecutionState();
        ExecutionState consumerExecutionState = responseHandle.getConsumerExecutionState();
        if (!isConsumerStateValidForConsumption(consumerExecutionState)) {
            LOG.debug(
                    "Ignore a partition producer state notification for task {}, because it's not running.",
                    taskNameWithSubtask);
        } else if (result.isLeft() || result.right() instanceof TimeoutException) {
            boolean isProducerConsumerReady = isProducerConsumerReady(responseHandle);
            if (isProducerConsumerReady) {
                return true;
            } else {
                abortConsumptionOrIgnoreCheckResult(responseHandle);
            }
        } else {
            handleFailedCheckResult(responseHandle);
        }
        return false;
    }

    private static boolean isConsumerStateValidForConsumption(
            ExecutionState consumerExecutionState) {
        return consumerExecutionState == ExecutionState.RUNNING
                || consumerExecutionState == ExecutionState.DEPLOYING;
    }

    private boolean isProducerConsumerReady(ResponseHandle responseHandle) {
        ExecutionState producerState = getProducerState(responseHandle);
        return producerState == ExecutionState.SCHEDULED
                || producerState == ExecutionState.DEPLOYING
                || producerState == ExecutionState.RUNNING
                || producerState == ExecutionState.FINISHED;
    }

    private void abortConsumptionOrIgnoreCheckResult(ResponseHandle responseHandle) {
        ExecutionState producerState = getProducerState(responseHandle);
        if (producerState == ExecutionState.CANCELING
                || producerState == ExecutionState.CANCELED
                || producerState == ExecutionState.FAILED) {

            // The producing execution has been canceled or failed. We
            // don't need to re-trigger the request since it cannot
            // succeed.
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Cancelling task {} after the producer of partition {} with attempt ID {} has entered state {}.",
                        taskNameWithSubtask,
                        resultPartitionId.getPartitionId(),
                        resultPartitionId.getProducerId(),
                        producerState);
            }

            responseHandle.cancelConsumption();
        } else {
            // Any other execution state is unexpected. Currently, only
            // state CREATED is left out of the checked states. If we
            // see a producer in this state, something went wrong with
            // scheduling in topological order.
            final String msg =
                    String.format(
                            "Producer with attempt ID %s of partition %s in unexpected state %s.",
                            resultPartitionId.getProducerId(),
                            resultPartitionId.getPartitionId(),
                            producerState);

            responseHandle.failConsumption(new IllegalStateException(msg));
        }
    }

    private static ExecutionState getProducerState(ResponseHandle responseHandle) {
        Either<ExecutionState, Throwable> result = responseHandle.getProducerExecutionState();
        return result.isLeft() ? result.left() : ExecutionState.RUNNING;
    }

    private void handleFailedCheckResult(ResponseHandle responseHandle) {
        Throwable throwable = responseHandle.getProducerExecutionState().right();
        if (throwable instanceof PartitionProducerDisposedException) {
            String msg =
                    String.format(
                            "Producer %s of partition %s disposed. Cancelling execution.",
                            resultPartitionId.getProducerId(), resultPartitionId.getPartitionId());
            LOG.info(msg, throwable);
            responseHandle.cancelConsumption();
        } else {
            responseHandle.failConsumption(throwable);
        }
    }
}
