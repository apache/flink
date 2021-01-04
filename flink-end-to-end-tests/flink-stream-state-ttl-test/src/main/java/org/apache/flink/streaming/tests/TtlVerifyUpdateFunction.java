/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.tests.verify.TtlStateVerifier;
import org.apache.flink.streaming.tests.verify.TtlUpdateContext;
import org.apache.flink.streaming.tests.verify.TtlVerificationContext;
import org.apache.flink.streaming.tests.verify.ValueWithTs;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Update state with TTL for each verifier.
 *
 * <p>This function for each verifier from {@link TtlStateVerifier#VERIFIERS} - creates state with
 * TTL - creates state of previous updates for further verification against it - receives random
 * state update - gets state value before update - updates state with random value - gets state
 * value after update - checks if this update clashes with any previous updates - if clashes, clears
 * state and recreate update - verifies last update against previous updates - emits verification
 * context in case of failure
 */
class TtlVerifyUpdateFunction extends RichFlatMapFunction<TtlStateUpdate, String>
        implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TtlVerifyUpdateFunction.class);

    @Nonnull private final StateTtlConfig ttlConfig;
    private final UpdateStat stat;

    private transient Map<String, State> states;
    private transient Map<String, ListState<ValueWithTs<?>>> prevUpdatesByVerifierId;

    TtlVerifyUpdateFunction(@Nonnull StateTtlConfig ttlConfig, long reportStatAfterUpdatesNum) {
        this.ttlConfig = ttlConfig;
        this.stat = new UpdateStat(reportStatAfterUpdatesNum);
    }

    @Override
    public void flatMap(TtlStateUpdate updates, Collector<String> out) throws Exception {
        for (TtlStateVerifier<?, ?> verifier : TtlStateVerifier.VERIFIERS) {
            TtlVerificationContext<?, ?> verificationContext =
                    generateUpdateAndVerificationContext(updates, verifier);
            if (!verifier.verify(verificationContext)) {
                // Please do **NOT** change the prefix, it's used in test_stream_state_ttl.sh for
                // test verifying
                out.collect("TTL verification failed: " + verificationContext.toString());
            }
        }
    }

    private TtlVerificationContext<?, ?> generateUpdateAndVerificationContext(
            TtlStateUpdate updates, TtlStateVerifier<?, ?> verifier) throws Exception {

        List<ValueWithTs<?>> prevUpdates = getPrevUpdates(verifier.getId());
        Object update = updates.getUpdate(verifier.getId());
        TtlUpdateContext<?, ?> updateContext = performUpdate(verifier, update);
        stat.update(prevUpdates.size());
        prevUpdatesByVerifierId.get(verifier.getId()).add(updateContext.getUpdateWithTs());
        return new TtlVerificationContext<>(
                updates.getKey(), verifier.getId(), prevUpdates, updateContext);
    }

    private List<ValueWithTs<?>> getPrevUpdates(String verifierId) throws Exception {
        return StreamSupport.stream(
                        prevUpdatesByVerifierId.get(verifierId).get().spliterator(), false)
                .collect(Collectors.toList());
    }

    private TtlUpdateContext<?, ?> performUpdate(TtlStateVerifier<?, ?> verifier, Object update)
            throws Exception {

        return MonotonicTTLTimeProvider.doWithFrozenTime(
                frozenTimestamp -> {
                    State state = states.get(verifier.getId());
                    Object valueBeforeUpdate = verifier.get(state);
                    verifier.update(state, update);
                    Object updatedValue = verifier.get(state);
                    return new TtlUpdateContext<>(
                            valueBeforeUpdate, update, updatedValue, frozenTimestamp);
                });
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {}

    @Override
    public void initializeState(FunctionInitializationContext context) {
        states =
                TtlStateVerifier.VERIFIERS.stream()
                        .collect(
                                Collectors.toMap(
                                        TtlStateVerifier::getId,
                                        v -> v.createState(context, ttlConfig)));
        prevUpdatesByVerifierId =
                TtlStateVerifier.VERIFIERS.stream()
                        .collect(
                                Collectors.toMap(
                                        TtlStateVerifier::getId,
                                        v -> {
                                            checkNotNull(v);
                                            final TypeSerializer<ValueWithTs<?>> typeSerializer =
                                                    new ValueWithTs.Serializer(
                                                            v.getUpdateSerializer(),
                                                            LongSerializer.INSTANCE);

                                            ListStateDescriptor<ValueWithTs<?>> stateDesc =
                                                    new ListStateDescriptor<>(
                                                            "TtlPrevValueState_" + v.getId(),
                                                            typeSerializer);
                                            KeyedStateStore store = context.getKeyedStateStore();
                                            return store.getListState(stateDesc);
                                        }));
    }

    private static class UpdateStat implements Serializable {
        private static final long serialVersionUID = -4557720969995878873L;

        final long reportStatAfterUpdatesNum;
        long updates = 0;
        long prevUpdatesNum = 0;

        UpdateStat(long reportStatAfterUpdatesNum) {
            this.reportStatAfterUpdatesNum = reportStatAfterUpdatesNum;
        }

        void update(long prevUpdatesSize) {
            updates++;
            prevUpdatesNum += prevUpdatesSize;
            if (updates % reportStatAfterUpdatesNum == 0) {
                LOG.info(String.format("Avg update chain length: %d", prevUpdatesNum / updates));
            }
        }
    }
}
