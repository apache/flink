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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateObject;

import org.apache.flink.shaded.guava18.com.google.common.io.Closer;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;

import static org.apache.flink.runtime.state.StateUtil.discardStateFuture;

/** Result of {@link StreamOperator#snapshotState}. */
public class OperatorSnapshotFutures {

    @Nonnull private RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateManagedFuture;

    @Nonnull private RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateRawFuture;

    @Nonnull private RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateManagedFuture;

    @Nonnull private RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateRawFuture;

    @Nonnull
    private Future<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>
            inputChannelStateFuture;

    @Nonnull
    private Future<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>
            resultSubpartitionStateFuture;

    public OperatorSnapshotFutures() {
        this(
                DoneFuture.of(SnapshotResult.empty()),
                DoneFuture.of(SnapshotResult.empty()),
                DoneFuture.of(SnapshotResult.empty()),
                DoneFuture.of(SnapshotResult.empty()),
                DoneFuture.of(SnapshotResult.empty()),
                DoneFuture.of(SnapshotResult.empty()));
    }

    public OperatorSnapshotFutures(
            @Nonnull RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateManagedFuture,
            @Nonnull RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateRawFuture,
            @Nonnull RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateManagedFuture,
            @Nonnull RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateRawFuture,
            @Nonnull
                    Future<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>
                            inputChannelStateFuture,
            @Nonnull
                    Future<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>
                            resultSubpartitionStateFuture) {
        this.keyedStateManagedFuture = keyedStateManagedFuture;
        this.keyedStateRawFuture = keyedStateRawFuture;
        this.operatorStateManagedFuture = operatorStateManagedFuture;
        this.operatorStateRawFuture = operatorStateRawFuture;
        this.inputChannelStateFuture = inputChannelStateFuture;
        this.resultSubpartitionStateFuture = resultSubpartitionStateFuture;
    }

    @Nonnull
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> getKeyedStateManagedFuture() {
        return keyedStateManagedFuture;
    }

    public void setKeyedStateManagedFuture(
            @Nonnull RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateManagedFuture) {
        this.keyedStateManagedFuture = keyedStateManagedFuture;
    }

    @Nonnull
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> getKeyedStateRawFuture() {
        return keyedStateRawFuture;
    }

    public void setKeyedStateRawFuture(
            @Nonnull RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateRawFuture) {
        this.keyedStateRawFuture = keyedStateRawFuture;
    }

    @Nonnull
    public RunnableFuture<SnapshotResult<OperatorStateHandle>> getOperatorStateManagedFuture() {
        return operatorStateManagedFuture;
    }

    public void setOperatorStateManagedFuture(
            @Nonnull
                    RunnableFuture<SnapshotResult<OperatorStateHandle>>
                            operatorStateManagedFuture) {
        this.operatorStateManagedFuture = operatorStateManagedFuture;
    }

    @Nonnull
    public RunnableFuture<SnapshotResult<OperatorStateHandle>> getOperatorStateRawFuture() {
        return operatorStateRawFuture;
    }

    public void setOperatorStateRawFuture(
            @Nonnull RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateRawFuture) {
        this.operatorStateRawFuture = operatorStateRawFuture;
    }

    @Nonnull
    public Future<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>
            getInputChannelStateFuture() {
        return inputChannelStateFuture;
    }

    public void setInputChannelStateFuture(
            @Nonnull
                    Future<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>
                            inputChannelStateFuture) {
        this.inputChannelStateFuture = inputChannelStateFuture;
    }

    @Nonnull
    public Future<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>
            getResultSubpartitionStateFuture() {
        return resultSubpartitionStateFuture;
    }

    public void setResultSubpartitionStateFuture(
            @Nonnull
                    Future<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>
                            resultSubpartitionStateFuture) {
        this.resultSubpartitionStateFuture = resultSubpartitionStateFuture;
    }

    public void cancel() throws Exception {
        List<Tuple2<Future<? extends StateObject>, String>> pairs = new ArrayList<>();
        pairs.add(new Tuple2<>(getKeyedStateManagedFuture(), "managed keyed"));
        pairs.add(new Tuple2<>(getKeyedStateRawFuture(), "managed operator"));
        pairs.add(new Tuple2<>(getOperatorStateManagedFuture(), "raw keyed"));
        pairs.add(new Tuple2<>(getOperatorStateRawFuture(), "raw operator"));
        pairs.add(new Tuple2<>(getInputChannelStateFuture(), "input channel"));
        pairs.add(new Tuple2<>(getResultSubpartitionStateFuture(), "result subpartition"));
        try (Closer closer = Closer.create()) {
            for (Tuple2<Future<? extends StateObject>, String> pair : pairs) {
                closer.register(
                        () -> {
                            try {
                                discardStateFuture(pair.f0);
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        String.format(
                                                "Could not properly cancel %s state future",
                                                pair.f1),
                                        e);
                            }
                        });
            }
        }
    }

    public Future<?>[] getAllFutures() {
        return new Future<?>[] {
            keyedStateManagedFuture,
            keyedStateRawFuture,
            operatorStateManagedFuture,
            operatorStateRawFuture,
            inputChannelStateFuture,
            resultSubpartitionStateFuture
        };
    }
}
