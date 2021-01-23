/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;

import javax.annotation.Nonnull;

import java.util.concurrent.ExecutionException;

import static org.apache.flink.runtime.checkpoint.StateObjectCollection.emptyIfNull;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singletonOrEmpty;

/**
 * This class finalizes {@link OperatorSnapshotFutures}. Each object is created with a {@link
 * OperatorSnapshotFutures} that is executed. The object can then deliver the results from the
 * execution as {@link OperatorSubtaskState}.
 */
public class OperatorSnapshotFinalizer {

    /** Primary replica of the operator subtask state for report to JM. */
    private final OperatorSubtaskState jobManagerOwnedState;

    /** Secondary replica of the operator subtask state for faster, local recovery on TM. */
    private final OperatorSubtaskState taskLocalState;

    public OperatorSnapshotFinalizer(@Nonnull OperatorSnapshotFutures snapshotFutures)
            throws ExecutionException, InterruptedException {

        SnapshotResult<KeyedStateHandle> keyedManaged =
                FutureUtils.runIfNotDoneAndGet(snapshotFutures.getKeyedStateManagedFuture());

        SnapshotResult<KeyedStateHandle> keyedRaw =
                FutureUtils.runIfNotDoneAndGet(snapshotFutures.getKeyedStateRawFuture());

        SnapshotResult<OperatorStateHandle> operatorManaged =
                FutureUtils.runIfNotDoneAndGet(snapshotFutures.getOperatorStateManagedFuture());

        SnapshotResult<OperatorStateHandle> operatorRaw =
                FutureUtils.runIfNotDoneAndGet(snapshotFutures.getOperatorStateRawFuture());

        SnapshotResult<StateObjectCollection<InputChannelStateHandle>> inputChannel =
                snapshotFutures.getInputChannelStateFuture().get();

        SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>> resultSubpartition =
                snapshotFutures.getResultSubpartitionStateFuture().get();

        jobManagerOwnedState =
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(
                                singletonOrEmpty(operatorManaged.getJobManagerOwnedSnapshot()))
                        .setRawOperatorState(
                                singletonOrEmpty(operatorRaw.getJobManagerOwnedSnapshot()))
                        .setManagedKeyedState(
                                singletonOrEmpty(keyedManaged.getJobManagerOwnedSnapshot()))
                        .setRawKeyedState(singletonOrEmpty(keyedRaw.getJobManagerOwnedSnapshot()))
                        .setInputChannelState(
                                emptyIfNull(inputChannel.getJobManagerOwnedSnapshot()))
                        .setResultSubpartitionState(
                                emptyIfNull(resultSubpartition.getJobManagerOwnedSnapshot()))
                        .build();

        taskLocalState =
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(
                                singletonOrEmpty(operatorManaged.getTaskLocalSnapshot()))
                        .setRawOperatorState(singletonOrEmpty(operatorRaw.getTaskLocalSnapshot()))
                        .setManagedKeyedState(singletonOrEmpty(keyedManaged.getTaskLocalSnapshot()))
                        .setRawKeyedState(singletonOrEmpty(keyedRaw.getTaskLocalSnapshot()))
                        .setInputChannelState(emptyIfNull(inputChannel.getTaskLocalSnapshot()))
                        .setResultSubpartitionState(
                                emptyIfNull(resultSubpartition.getTaskLocalSnapshot()))
                        .build();
    }

    public OperatorSubtaskState getTaskLocalState() {
        return taskLocalState;
    }

    public OperatorSubtaskState getJobManagerOwnedState() {
        return jobManagerOwnedState;
    }
}
