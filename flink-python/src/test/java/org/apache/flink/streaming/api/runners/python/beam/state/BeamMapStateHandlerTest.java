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

package org.apache.flink.streaming.api.runners.python.beam.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateLatencyTrackOptions;
import org.apache.flink.fnexecution.v1.FlinkFnApi.StateDescriptor;
import org.apache.flink.fnexecution.v1.FlinkFnApi.StateDescriptor.StateTTLConfig;
import org.apache.flink.fnexecution.v1.FlinkFnApi.StateDescriptor.StateTTLConfig.CleanupStrategies;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;
import org.apache.flink.streaming.api.utils.ByteArrayWrapperSerializer;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the map-state protocol against a real heap backend and a controllable TTL clock. */
class BeamMapStateHandlerTest {

    @ParameterizedTest
    @CsvSource({
        "ITEMS, false, ReturnExpiredIfNotCleanedUp",
        "KEYS, false, ReturnExpiredIfNotCleanedUp",
        "VALUES, false, ReturnExpiredIfNotCleanedUp",
        "ITEMS, true, ReturnExpiredIfNotCleanedUp",
        "KEYS, true, ReturnExpiredIfNotCleanedUp",
        "VALUES, true, ReturnExpiredIfNotCleanedUp",
        "ITEMS, false, NeverReturnExpired",
        "KEYS, false, NeverReturnExpired",
        "VALUES, false, NeverReturnExpired",
        "ITEMS, true, NeverReturnExpired",
        "KEYS, true, NeverReturnExpired",
        "VALUES, true, NeverReturnExpired"
    })
    void testMixedExpiryDoesNotInvalidateActiveIterator(
            IterateType type, boolean metrics, StateTtlConfig.StateVisibility visibility)
            throws Exception {
        try (Fixture fixture = new Fixture(metrics, visibility)) {
            for (int key = 0; key < 64; key++) {
                fixture.state.put(key(key), new byte[] {(byte) key});
            }
            List<Integer> order = new ArrayList<>();
            for (ByteArrayWrapper key : fixture.state.keys()) {
                order.add(Byte.toUnsignedInt(key.get(0)));
            }
            fixture.harness.setStateTtlProcessingTime(101);
            for (int key : order.subList(0, 8)) {
                fixture.state.put(key(key), new byte[] {(byte) key});
            }

            BeamFnApi.StateGetResponse page = iterate(fixture, type, ByteString.EMPTY);
            List<Integer> firstPage = decodeKeys(page, type);
            assertThat(firstPage).containsExactlyElementsOf(order.subList(0, 2));
            assertThat(page.getContinuationToken()).isNotEqualTo(ByteString.EMPTY);
            assertThat(checkEmptyExcluding(fixture, Collections.singletonList(firstPage.get(0))))
                    .isFalse();

            List<Integer> visited = new ArrayList<>(firstPage);
            while (!page.getContinuationToken().isEmpty()) {
                page = iterate(fixture, type, page.getContinuationToken());
                visited.addAll(decodeKeys(page, type));
            }
            // ReturnExpiredIfNotCleanedUp permits expired entries to be returned during cleanup.
            List<Integer> expected =
                    visibility == StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp
                            ? order
                            : order.subList(0, 8);
            assertThat(visited).containsExactlyElementsOf(expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testExcludedKeysAndNullValues(boolean metrics) throws Exception {
        try (Fixture fixture =
                new Fixture(metrics, StateTtlConfig.StateVisibility.NeverReturnExpired)) {
            fixture.state.put(key(1), null);
            fixture.state.put(key(2), new byte[] {2});
            assertThat(checkEmptyExcluding(fixture, Collections.emptyList())).isFalse();
            assertThat(checkEmptyExcluding(fixture, Collections.singletonList(2))).isFalse();
            assertThat(checkEmptyExcluding(fixture, List.of(1, 2))).isTrue();
            fixture.state.clear();
            assertThat(checkEmptyExcluding(fixture, Collections.emptyList())).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCheckingEmptyDoesNotRefreshTtl(boolean metrics) throws Exception {
        try (Fixture fixture =
                new Fixture(metrics, StateTtlConfig.StateVisibility.NeverReturnExpired)) {
            fixture.state.put(key(1), new byte[] {1});
            fixture.harness.setStateTtlProcessingTime(50);
            assertThat(checkEmptyExcluding(fixture, Collections.singletonList(2))).isFalse();
            fixture.harness.setStateTtlProcessingTime(101);
            assertThat(fixture.state.get(key(1))).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEmptyExclusionsDoNotRunIncrementalCleanup(boolean metrics) throws Exception {
        try (Fixture fixture =
                new Fixture(metrics, StateTtlConfig.StateVisibility.NeverReturnExpired, true)) {
            fixture.state.put(key(1), new byte[] {1});
            fixture.harness.setStateTtlProcessingTime(101);
            assertThat(checkEmptyExcluding(fixture, Collections.emptyList())).isTrue();
            // A read-only query reports expiration while retaining the physical state entry.
            assertThat(fixture.harness.numKeyedStateEntries()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testReadOnlyRpcDoesNotTriggerPerRecordCleanup(boolean metrics) throws Exception {
        try (Fixture fixture =
                new Fixture(
                        metrics, StateTtlConfig.StateVisibility.NeverReturnExpired, false, 16)) {
            BeamStateRequestHandler requests =
                    BeamStateRequestHandler.of(
                            fixture.backend, null, IntSerializer.INSTANCE, null, fixture.config);
            AtomicInteger selections = new AtomicInteger();
            fixture.backend.registerKeySelectionListener(ignored -> selections.incrementAndGet());
            BeamFnApi.StateKey firstKey = rpcStateKey(0);
            BeamFnApi.StateKey secondKey = rpcStateKey(1);
            appendThroughStore(requests, firstKey);
            appendThroughStore(requests, secondKey);
            BeamFnApi.StateGetResponse page =
                    getThroughStore(
                            requests,
                            firstKey,
                            iterateRequest(IterateType.ITEMS, ByteString.EMPTY));
            assertThat(page.getContinuationToken()).isNotEqualTo(ByteString.EMPTY);
            int beforeQuery = selections.get();
            assertThat(beforeQuery).isEqualTo(3);
            fixture.harness.setStateTtlProcessingTime(101);

            for (int key : new int[] {0, 1, 0}) {
                BeamFnApi.StateGetResponse response =
                        getThroughStore(
                                requests,
                                rpcStateKey(key),
                                checkEmptyExcludingRequest(Collections.emptyList()));
                assertThat(response.getData()).isEqualTo(ByteString.copyFrom(new byte[] {3}));
                assertThat(selections.get()).isEqualTo(beforeQuery);
                assertThat(fixture.backend.getCurrentKey()).isEqualTo(key);
                assertThat(fixture.backend.getCurrentKeyGroupIndex())
                        .isEqualTo(KeyGroupRangeAssignment.assignToKeyGroup(key, 16));
                assertThat(fixture.harness.numKeyedStateEntries()).isEqualTo(2);
            }

            int ordinaryRequests = 0;
            while (!page.getContinuationToken().isEmpty()) {
                page =
                        getThroughStore(
                                requests,
                                firstKey,
                                iterateRequest(IterateType.ITEMS, page.getContinuationToken()));
                ordinaryRequests++;
            }
            assertThat(ordinaryRequests).isPositive();
            assertThat(selections.get()).isEqualTo(beforeQuery + ordinaryRequests);
        }
    }

    private static BeamFnApi.StateKey rpcStateKey(int key) throws Exception {
        CleanupStrategies cleanup =
                CleanupStrategies.newBuilder()
                        .setIsCleanupInBackground(false)
                        .addStrategies(
                                CleanupStrategies.MapStrategiesEntry.newBuilder()
                                        .setStrategy(
                                                CleanupStrategies.Strategies.INCREMENTAL_CLEANUP)
                                        .setIncrementalCleanupStrategy(
                                                CleanupStrategies.IncrementalCleanupStrategy
                                                        .newBuilder()
                                                        .setCleanupSize(1)
                                                        .setRunCleanupForEveryRecord(true)))
                        .build();
        StateDescriptor descriptor =
                StateDescriptor.newBuilder()
                        .setStateName("rpc-map")
                        .setStateTtlConfig(
                                StateTTLConfig.newBuilder()
                                        .setTtl(100)
                                        .setUpdateType(StateTTLConfig.UpdateType.OnReadAndWrite)
                                        .setStateVisibility(
                                                StateTTLConfig.StateVisibility.NeverReturnExpired)
                                        .setCleanupStrategies(cleanup))
                        .build();
        ByteArrayOutputStream keyBytes = new ByteArrayOutputStream();
        new DataOutputStream(keyBytes).writeInt(key);
        return BeamFnApi.StateKey.newBuilder()
                .setMultimapSideInput(
                        BeamFnApi.StateKey.MultimapSideInput.newBuilder()
                                .setKey(ByteString.copyFrom(keyBytes.toByteArray()))
                                .setSideInputId(
                                        Base64.getEncoder()
                                                .encodeToString(descriptor.toByteArray())))
                .build();
    }

    private static void appendThroughStore(BeamStateRequestHandler handler, BeamFnApi.StateKey key)
            throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(bytes);
        data.writeInt(4);
        for (int mapKey = 0; mapKey < 4; mapKey++) {
            data.writeByte(2);
            data.writeInt(1);
            data.writeByte(mapKey);
            data.writeInt(1);
            data.writeByte(mapKey);
        }
        handler.handle(
                        BeamFnApi.StateRequest.newBuilder()
                                .setId("append")
                                .setStateKey(key)
                                .setAppend(
                                        BeamFnApi.StateAppendRequest.newBuilder()
                                                .setData(ByteString.copyFrom(bytes.toByteArray())))
                                .build())
                .toCompletableFuture()
                .get();
    }

    private static BeamFnApi.StateGetResponse getThroughStore(
            BeamStateRequestHandler handler, BeamFnApi.StateKey key, byte[] request)
            throws Exception {
        return handler.handle(
                        BeamFnApi.StateRequest.newBuilder()
                                .setId("get")
                                .setStateKey(key)
                                .setGet(
                                        BeamFnApi.StateGetRequest.newBuilder()
                                                .setContinuationToken(ByteString.copyFrom(request)))
                                .build())
                .toCompletableFuture()
                .get()
                .build()
                .getGet();
    }

    private static boolean checkEmptyExcluding(Fixture fixture, List<Integer> excluded)
            throws Exception {
        BeamFnApi.StateGetResponse response = get(fixture, checkEmptyExcludingRequest(excluded));
        assertThat(response.getContinuationToken()).isEqualTo(ByteString.EMPTY);
        assertThat(response.getData().size()).isEqualTo(1);
        assertThat(response.getData().byteAt(0)).isIn((byte) 3, (byte) 4);
        return response.getData().byteAt(0) == 3;
    }

    private static byte[] checkEmptyExcludingRequest(List<Integer> excluded) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream request = new DataOutputStream(bytes);
        request.writeByte(3);
        request.writeInt(excluded.size());
        for (int key : excluded) {
            request.writeInt(1);
            request.writeByte(key);
        }
        return bytes.toByteArray();
    }

    private static BeamFnApi.StateGetResponse iterate(
            Fixture fixture, IterateType type, ByteString token) throws Exception {
        return get(fixture, iterateRequest(type, token));
    }

    private static byte[] iterateRequest(IterateType type, ByteString token) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream request = new DataOutputStream(bytes);
        request.writeByte(1);
        request.writeByte(type.getOrd());
        request.writeInt(token.size());
        token.writeTo(request);
        return bytes.toByteArray();
    }

    private static BeamFnApi.StateGetResponse get(Fixture fixture, byte[] request)
            throws Exception {
        return fixture.handler
                .handleGet(
                        BeamFnApi.StateRequest.newBuilder()
                                .setId("request")
                                .setGet(
                                        BeamFnApi.StateGetRequest.newBuilder()
                                                .setContinuationToken(ByteString.copyFrom(request)))
                                .build(),
                        fixture.state)
                .build()
                .getGet();
    }

    private static List<Integer> decodeKeys(BeamFnApi.StateGetResponse response, IterateType type)
            throws Exception {
        DataInputStream input =
                new DataInputStream(new ByteArrayInputStream(response.getData().toByteArray()));
        List<Integer> keys = new ArrayList<>();
        while (input.available() > 0) {
            keys.add(input.readUnsignedByte());
            if (type != IterateType.KEYS && input.readBoolean()) {
                input.readByte();
            }
        }
        return keys;
    }

    private static ByteArrayWrapper key(int key) {
        return new ByteArrayWrapper(new byte[] {(byte) key});
    }

    private static class Fixture implements AutoCloseable {
        private final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> harness;
        private final MapState<ByteArrayWrapper, byte[]> state;
        private final BeamMapStateHandler handler;
        private final AbstractKeyedStateBackend<Integer> backend;
        private final Configuration config;

        private Fixture(boolean metrics, StateTtlConfig.StateVisibility visibility)
                throws Exception {
            this(metrics, visibility, false);
        }

        private Fixture(
                boolean metrics, StateTtlConfig.StateVisibility visibility, boolean incremental)
                throws Exception {
            this(metrics, visibility, incremental, 1);
        }

        private Fixture(
                boolean metrics,
                StateTtlConfig.StateVisibility visibility,
                boolean incremental,
                int maxParallelism)
                throws Exception {
            config = new Configuration();
            config.set(PythonOptions.MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE, 2);
            config.set(StateLatencyTrackOptions.LATENCY_TRACK_ENABLED, metrics);
            config.set(StateLatencyTrackOptions.LATENCY_TRACK_SAMPLE_INTERVAL, 1);
            StreamMap<Integer, Integer> operator = new StreamMap<>(value -> value);
            harness =
                    new KeyedOneInputStreamOperatorTestHarness<>(
                            operator, value -> 0, Types.INT, maxParallelism, 1, 0);
            harness.setStateBackend(
                    new HashMapStateBackend()
                            .configure(config, Thread.currentThread().getContextClassLoader()));
            harness.open();
            harness.setStateTtlProcessingTime(0);
            backend = (AbstractKeyedStateBackend<Integer>) operator.<Integer>getKeyedStateBackend();
            backend.setCurrentKey(0);
            MapStateDescriptor<ByteArrayWrapper, byte[]> descriptor =
                    new MapStateDescriptor<>(
                            "map",
                            ByteArrayWrapperSerializer.INSTANCE,
                            BytePrimitiveArraySerializer.INSTANCE);
            StateTtlConfig.Builder ttl =
                    StateTtlConfig.newBuilder(Duration.ofMillis(100))
                            .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                            .setStateVisibility(visibility)
                            .disableCleanupInBackground();
            if (incremental) {
                ttl.cleanupIncrementally(1, false);
            }
            descriptor.enableTimeToLive(ttl.build());
            state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);
            if (metrics) {
                assertThat(state.getClass().getSimpleName()).isEqualTo("MetricsTrackingMapState");
            }
            handler = new BeamMapStateHandler(config);
        }

        @Override
        public void close() throws Exception {
            harness.close();
        }
    }
}
