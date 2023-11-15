/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.changelog;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.state.changelog.StateChange.META_KEY_GROUP;
import static org.apache.flink.state.changelog.StateChangeOperation.METADATA;
import static org.junit.Assert.assertEquals;

abstract class StateChangeLoggerTestBase<Namespace> {
    /** A basic test for appending the metadata on first state access. */
    @Test
    public void testMetadataOperationLogged() throws IOException {
        TestingStateChangelogWriter writer = new TestingStateChangelogWriter();
        InternalKeyContextImpl<String> keyContext =
                new InternalKeyContextImpl<>(KeyGroupRange.of(1, 1000), 1000);

        try (StateChangeLogger<String, Namespace> logger = getLogger(writer, keyContext)) {
            List<Tuple2<Integer, StateChangeOperation>> expectedAppends = new ArrayList<>();
            expectedAppends.add(Tuple2.of(META_KEY_GROUP, METADATA));

            // log every applicable operations, several times each
            int numOpTypes = StateChangeOperation.values().length;
            for (int i = 0; i < numOpTypes * 7; i++) {
                String element = Integer.toString(i);
                StateChangeOperation operation =
                        StateChangeOperation.byCode((byte) (i % numOpTypes));
                log(operation, element, logger, keyContext).ifPresent(expectedAppends::add);
            }
            assertEquals(expectedAppends, writer.appends);
        }
    }

    protected abstract StateChangeLogger<String, Namespace> getLogger(
            TestingStateChangelogWriter writer, InternalKeyContextImpl<String> keyContext);

    protected Optional<Tuple2<Integer, StateChangeOperation>> log(
            StateChangeOperation op,
            String element,
            StateChangeLogger<String, Namespace> logger,
            InternalKeyContextImpl<String> keyContext)
            throws IOException {
        keyContext.setCurrentKey(element);
        Namespace namespace = getNamespace(element);
        switch (op) {
            case ADD:
                logger.valueAdded(element, namespace);
                break;
            case ADD_ELEMENT:
                logger.valueElementAdded(w -> {}, namespace);
                break;
            case REMOVE_ELEMENT:
                logger.valueElementRemoved(w -> {}, namespace);
                break;
            case CLEAR:
                logger.valueCleared(namespace);
                break;
            case SET:
                logger.valueUpdated(element, namespace);
                break;
            case SET_INTERNAL:
                logger.valueUpdatedInternal(element, namespace);
                break;
            case ADD_OR_UPDATE_ELEMENT:
                logger.valueElementAddedOrUpdated(w -> {}, namespace);
                break;
            default:
                return Optional.empty();
        }
        return Optional.of(Tuple2.of(keyContext.getCurrentKeyGroupIndex(), op));
    }

    protected abstract Namespace getNamespace(String element);

    @SuppressWarnings("rawtypes")
    protected static class TestingStateChangelogWriter implements StateChangelogWriter {
        private final List<Tuple2<Integer, StateChangeOperation>> appends = new ArrayList<>();

        @Override
        public void appendMeta(byte[] value) {
            appends.add(Tuple2.of(META_KEY_GROUP, StateChangeOperation.byCode(value[0])));
        }

        @Override
        public void append(int keyGroup, byte[] value) {
            appends.add(Tuple2.of(keyGroup, StateChangeOperation.byCode(value[0])));
        }

        @Override
        public SequenceNumber initialSequenceNumber() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SequenceNumber nextSequenceNumber() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<?> persist(SequenceNumber from, long checkpointId)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void truncate(SequenceNumber to) {}

        @Override
        public void confirm(SequenceNumber from, SequenceNumber to, long checkpointId) {}

        @Override
        public void reset(SequenceNumber from, SequenceNumber to, long checkpointId) {}

        @Override
        public void truncateAndClose(SequenceNumber from) {}

        @Override
        public void close() {}
    }
}
