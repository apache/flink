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
import org.apache.flink.runtime.checkpoint.StateHandleDummyUtil;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.deepDummyCopy;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton;
import static org.apache.flink.runtime.state.SnapshotResult.withLocalState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link OperatorSnapshotFinalizer}. */
public class OperatorSnapshotFinalizerTest extends TestLogger {

    /** Test that the runnable futures are executed and the result is correctly extracted. */
    @Test
    public void testRunAndExtract() throws Exception {

        Random random = new Random(0x42);

        KeyedStateHandle keyedTemplate =
                StateHandleDummyUtil.createNewKeyedStateHandle(new KeyGroupRange(0, 0));
        OperatorStateHandle operatorTemplate =
                StateHandleDummyUtil.createNewOperatorStateHandle(2, random);
        InputChannelStateHandle inputChannelTemplate =
                StateHandleDummyUtil.createNewInputChannelStateHandle(2, random);
        ResultSubpartitionStateHandle resultSubpartitionTemplate =
                StateHandleDummyUtil.createNewResultSubpartitionStateHandle(2, random);

        SnapshotResult<KeyedStateHandle> manKeyed =
                withLocalState(deepDummyCopy(keyedTemplate), deepDummyCopy(keyedTemplate));
        SnapshotResult<KeyedStateHandle> rawKeyed =
                withLocalState(deepDummyCopy(keyedTemplate), deepDummyCopy(keyedTemplate));
        SnapshotResult<OperatorStateHandle> manOper =
                withLocalState(deepDummyCopy(operatorTemplate), deepDummyCopy(operatorTemplate));
        SnapshotResult<OperatorStateHandle> rawOper =
                withLocalState(deepDummyCopy(operatorTemplate), deepDummyCopy(operatorTemplate));
        SnapshotResult<StateObjectCollection<InputChannelStateHandle>> inputChannel =
                withLocalState(
                        singleton(deepDummyCopy(inputChannelTemplate)),
                        singleton(deepDummyCopy(inputChannelTemplate)));
        SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>> resultSubpartition =
                withLocalState(
                        singleton(deepDummyCopy(resultSubpartitionTemplate)),
                        singleton(deepDummyCopy(resultSubpartitionTemplate)));

        OperatorSnapshotFutures snapshotFutures =
                new OperatorSnapshotFutures(
                        new PseudoNotDoneFuture<>(manKeyed),
                        new PseudoNotDoneFuture<>(rawKeyed),
                        new PseudoNotDoneFuture<>(manOper),
                        new PseudoNotDoneFuture<>(rawOper),
                        new PseudoNotDoneFuture<>(inputChannel),
                        new PseudoNotDoneFuture<>(resultSubpartition));

        for (Future<?> f : snapshotFutures.getAllFutures()) {
            assertFalse(f.isDone());
        }

        OperatorSnapshotFinalizer finalizer = new OperatorSnapshotFinalizer(snapshotFutures);

        for (Future<?> f : snapshotFutures.getAllFutures()) {
            assertTrue(f.isDone());
        }

        Map<SnapshotResult<?>, Function<OperatorSubtaskState, ? extends StateObject>> map =
                new HashMap<>();
        map.put(manKeyed, headExtractor(OperatorSubtaskState::getManagedKeyedState));
        map.put(rawKeyed, headExtractor(OperatorSubtaskState::getRawKeyedState));
        map.put(manOper, headExtractor(OperatorSubtaskState::getManagedOperatorState));
        map.put(rawOper, headExtractor(OperatorSubtaskState::getRawOperatorState));
        map.put(inputChannel, OperatorSubtaskState::getInputChannelState);
        map.put(resultSubpartition, OperatorSubtaskState::getResultSubpartitionState);

        for (Map.Entry<SnapshotResult<?>, Function<OperatorSubtaskState, ? extends StateObject>> e :
                map.entrySet()) {
            assertEquals(
                    e.getKey().getJobManagerOwnedSnapshot(),
                    e.getValue().apply(finalizer.getJobManagerOwnedState()));
        }
        for (Map.Entry<SnapshotResult<?>, Function<OperatorSubtaskState, ? extends StateObject>> e :
                map.entrySet()) {
            assertEquals(
                    e.getKey().getTaskLocalSnapshot(),
                    e.getValue().apply(finalizer.getTaskLocalState()));
        }
    }

    private static <T extends StateObject> Function<OperatorSubtaskState, T> headExtractor(
            Function<OperatorSubtaskState, StateObjectCollection<T>> collectionExtractor) {
        return collectionExtractor.andThen(
                col -> col == null || col.isEmpty() ? null : col.iterator().next());
    }

    private void checkResult(Object expected, StateObjectCollection<?> actual) {
        if (expected == null) {
            assertTrue(actual == null || actual.isEmpty());
        } else {
            assertEquals(1, actual.size());
            assertEquals(expected, actual.iterator().next());
        }
    }

    static class PseudoNotDoneFuture<T> extends DoneFuture<T> {

        private boolean done;

        PseudoNotDoneFuture(T payload) {
            super(payload);
            this.done = false;
        }

        @Override
        public void run() {
            super.run();
            this.done = true;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public T get() {
            try {
                return super.get();
            } finally {
                this.done = true;
            }
        }
    }
}
