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

package org.apache.flink.table.runtime.sequencedmultisetstate;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.SerializerFactory;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.StateChangeInfo;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.StateChangeType;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.Strategy;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetStateContext.KeyExtractor;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.TriFunctionWithException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.LongStream;

import static org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.StateChangeType.REMOVAL_ALL;
import static org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.StateChangeType.REMOVAL_LAST_ADDED;
import static org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.StateChangeType.REMOVAL_NOT_FOUND;
import static org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.StateChangeType.REMOVAL_OTHER;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.rowOfKind;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Test for various implementations of {@link SequencedMultiSetState}. */
@SuppressWarnings({"SameParameterValue", "unused"})
@ExtendWith(ParameterizedTestExtension.class)
public class SequencedMultiSetStateTest {

    @Parameter(0)
    private Strategy strategy;

    @Parameter(1)
    private long adaptiveLowThresholdOverride;

    @Parameter(2)
    private long adaptiveHighThresholdOverride;

    @Parameters(name = "strategy={0}, lowThreshold={1}, highThreshold={2}")
    public static Object[][] parameters() {
        return new Object[][] {
            new Object[] {Strategy.VALUE_STATE, -1, -1},
            new Object[] {Strategy.MAP_STATE, -1, -1},
            new Object[] {Strategy.ADAPTIVE, 0, 1},
            new Object[] {Strategy.ADAPTIVE, 1, 2},
            new Object[] {Strategy.ADAPTIVE, 0, 10},
            new Object[] {Strategy.ADAPTIVE, 9, 10},
        };
    }

    // for simplicity, all tests use string type only, with row key being the 1st column
    private static final LogicalType VARCHAR = DataTypes.VARCHAR(50).getLogicalType();
    public static final int KEY_POS = 0;

    @TestTemplate
    public void testBasicFlow() throws Exception {
        runTest(
                (state, keyContext) -> {
                    keyContext.setCurrentKey("sk1");
                    assertTrue(state.isEmpty());

                    state.add(row("key", "value"), 1L);
                    assertFalse(state.isEmpty());

                    keyContext.setCurrentKey("sk2");
                    assertTrue(state.isEmpty());

                    keyContext.setCurrentKey("sk1");
                    state.clear();
                    assertStateContents(state);
                });
    }

    @TestTemplate
    public void testAppend() throws Exception {
        runTest(
                state -> {
                    // should always keep appending
                    state.append(row("k1", "x"), 777L);
                    assertStateContents(state, Tuple2.of(row("k1", "x"), 777L));

                    state.append(row("k1", "x"), 778L);
                    assertStateContents(
                            state,
                            Tuple2.of(row("k1", "x"), 777L),
                            Tuple2.of(row("k1", "x"), 778L));

                    state.append(row("k2", "y"), 779L);
                    assertStateContents(
                            state,
                            Tuple2.of(row("k1", "x"), 777L),
                            Tuple2.of(row("k1", "x"), 778L),
                            Tuple2.of(row("k2", "y"), 779L));

                    state.append(row("k1", "x"), 777L);
                    assertStateContents(
                            state,
                            Tuple2.of(row("k1", "x"), 777L),
                            Tuple2.of(row("k1", "x"), 778L),
                            Tuple2.of(row("k2", "y"), 779L),
                            Tuple2.of(row("k1", "x"), 777L));
                });
    }

    @TestTemplate
    public void testAdd() throws Exception {
        runTest(
                state -> {
                    state.add(row("k1", "x"), 777L);
                    assertStateContents(state, row("k1", "x"), 777L);

                    state.add(row("k2", "x"), 777L);
                    assertStateContents(
                            state,
                            Tuple2.of(row("k1", "x"), 777L),
                            Tuple2.of(row("k2", "x"), 777L));

                    state.add(row("k2", "y"), 778L);
                    assertStateContents(
                            state,
                            Tuple2.of(row("k1", "x"), 777L),
                            Tuple2.of(row("k2", "y"), 778L));

                    state.add(row("k1", "y"), 778L);
                    assertStateContents(
                            state,
                            Tuple2.of(row("k1", "y"), 778L),
                            Tuple2.of(row("k2", "y"), 778L));
                });
    }

    @TestTemplate
    public void testRemove() throws Exception {
        runTest(
                state -> {
                    removeAndAssert(state, row("key1"), REMOVAL_NOT_FOUND);

                    state.add(row("key1", "value"), 777L);
                    state.add(row("key2", "value"), 777L);
                    state.add(row("key3", "value"), 777L);
                    state.add(row("key4", "value"), 777L);

                    removeAndAssert(state, row("key999"), REMOVAL_NOT_FOUND);
                    removeAndAssert(state, row("key4"), REMOVAL_LAST_ADDED, row("key3", "value"));
                    removeAndAssert(state, row("key3"), REMOVAL_LAST_ADDED, row("key2", "value"));
                    removeAndAssert(state, row("key1"), REMOVAL_OTHER);
                    removeAndAssert(
                            state,
                            row("key2", "value-to-return"),
                            REMOVAL_ALL,
                            // value-to-return should be returned, not the original value
                            // according to the current logic of Flink operators
                            row("key2", "value-to-return"));

                    // shouldn't fail e.g. due to bad pointers
                    removeAndAssert(state, row("key1"), REMOVAL_NOT_FOUND);
                    removeAndAssert(state, row("key2"), REMOVAL_NOT_FOUND);
                });
    }

    @TestTemplate
    public void testAddAfterRemovingTail() throws Exception {
        runTest(
                state -> {
                    state.add(row("key1", "value-1"), 777L);
                    state.add(row("key2", "value-2"), 777L);

                    // key1 is the tail now - remove it and then add
                    removeAndAssert(state, row("key1"), REMOVAL_OTHER, row("key1", "value-1"));
                    state.add(row("key1", "value-1"), 777L);

                    // key2 is the tail now - remove it and then add
                    removeAndAssert(state, row("key2"), REMOVAL_OTHER, row("key2", "value-2"));
                    state.add(row("key2", "value-2"), 777L);
                });
    }

    @TestTemplate
    public void testRemoveFirstAppended() throws Exception {
        runTest(
                state -> {
                    state.append(row("key", "value-1"), 777L);
                    state.append(row("key", "value-2"), 778L);
                    state.append(row("key", "value-3"), 779L);

                    removeAndAssert(state, row("key"), REMOVAL_OTHER);
                    assertStateContents(
                            state,
                            Tuple2.of(row("key", "value-2"), 778L),
                            Tuple2.of(row("key", "value-3"), 779L));

                    removeAndAssert(state, row("key"), REMOVAL_OTHER);
                    assertStateContents(state, Tuple2.of(row("key", "value-3"), 779L));

                    removeAndAssert(state, row("key"), REMOVAL_ALL, row("key"));
                    assertTrue(state.isEmpty());
                });
    }

    @TestTemplate
    public void testRemoveWithInterleavingRowAppended() throws Exception {
        runTest(
                state -> {
                    state.append(row("key1", "value"), 777L); // sqn = 1
                    state.append(row("key2", "value"), 777L); // sqn = 2
                    state.append(row("key2", "value"), 778L); // sqn = 3
                    removeAndAssert(state, row("key2"), REMOVAL_OTHER, row("key2", "value"));
                    removeAndAssert(state, row("key2"), REMOVAL_LAST_ADDED, row("key1", "value"));
                    removeAndAssert(
                            state,
                            row("key1", "value-to-return"),
                            REMOVAL_ALL,
                            row("key1", "value-to-return"));
                });
    }

    /** Test that loading and clearing the cache doesn't impact correctness. */
    @TestTemplate
    public void testCaching() throws Exception {
        runTest(
                (state, ctx) -> {
                    ctx.setCurrentKey("sk1");
                    state.add(row("key", "value-1"), 777L);
                    state.clearCache();
                    assertFalse(state.isEmpty());

                    ctx.setCurrentKey("sk2");
                    state.loadCache();
                    assertTrue(state.isEmpty());
                });
    }

    /** Test that loading and clearing the cache doesn't impact correctness. */
    @TestTemplate
    public void testKeyExtraction() throws Exception {
        final KeyExtractor keyExtractor =
                row -> ProjectedRowData.from(new int[] {1}).replaceRow(row);

        runTest(
                (state, ctx) -> {
                    ctx.setCurrentKey("sk1");
                    state.add(row("value-123", "key"), 777L);
                    assertFalse(state.isEmpty());
                    StateChangeInfo<RowData> ret = state.remove(row("value-456", "key"));
                    Tuple3.of(ret.getSizeAfter(), ret.getChangeType(), ret.getPayload());
                    assertTrue(state.isEmpty());
                },
                keyExtractor,
                0);
    }

    /** Test that row kind is not taken into account when matching the rows. */
    @TestTemplate
    public void testRowKindNormalization() throws Exception {
        runTest(
                state -> {
                    for (RowKind firstKind : RowKind.values()) {
                        for (RowKind secondKind : RowKind.values()) {

                            state.append(rowOfKind(firstKind, "key", "value"), 778L);
                            state.remove(rowOfKind(secondKind, "key", "value"));
                            assertTrue(state.isEmpty());

                            state.add(rowOfKind(firstKind, "key", "value"), 777L);
                            state.remove(rowOfKind(secondKind, "key", "value"));
                            assertTrue(state.isEmpty());

                            state.add(rowOfKind(firstKind, "key", "value"), 777L);
                            state.add(rowOfKind(secondKind, "key", "value"), 778L);
                            assertStateContents(state, Tuple2.of(row("key", "value"), 778L));
                            state.clear();
                        }
                    }
                });
    }

    @TestTemplate
    public void testAdaptivity() throws Exception {
        assumeTrue(strategy == Strategy.ADAPTIVE);
        final long totalSize = adaptiveHighThresholdOverride * 2;
        runTest(
                (state, ctx) -> {
                    AdaptiveSequencedMultiSetState ad = (AdaptiveSequencedMultiSetState) state;
                    int runningSize = 0;

                    ctx.setCurrentKey("k1");
                    assertFalse(ad.isIsUsingLargeState(), "should start with value state");
                    for (; runningSize < totalSize; runningSize++) {
                        assertEquals(
                                runningSize >= adaptiveHighThresholdOverride,
                                ad.isIsUsingLargeState(),
                                "should switch after reaching high threshold");
                        ad.append(row("key", "value"), runningSize /* timestamp */);
                    }

                    ctx.setCurrentKey("k2");
                    assertFalse(ad.isIsUsingLargeState(), "should not mix different context keys");

                    ctx.setCurrentKey("k1");
                    assertTrue(ad.isIsUsingLargeState(), "should not mix different context keys");

                    // remove until hitting the threshold - shouldn't trigger switch
                    for (; runningSize > adaptiveLowThresholdOverride + 1; runningSize--) {
                        ad.remove(row("key"));
                        assertTrue(
                                ad.isIsUsingLargeState(),
                                "should switch back after reaching low threshold");
                    }
                    // trigger switch
                    ad.remove(row("key"));
                    runningSize--;
                    assertFalse(
                            ad.isIsUsingLargeState(),
                            "should switch back after reaching low threshold");
                    // verify the order of the migrated elements by looking at their timestamps
                    //noinspection unchecked
                    assertStateContents(
                            state,
                            LongStream.range(totalSize - runningSize, totalSize)
                                    .mapToObj(ts -> Tuple2.of(row("key", "value"), ts))
                                    .toArray(Tuple2[]::new));
                    for (; runningSize > 0; runningSize--) {
                        assertFalse(
                                ad.isIsUsingLargeState(),
                                "should switch back after reaching low threshold");
                        ad.remove(row("key"));
                    }
                    assertTrue(ad.isEmpty());
                    assertEquals(0, runningSize);

                    for (; runningSize < totalSize; runningSize++) {
                        assertEquals(
                                runningSize >= adaptiveHighThresholdOverride,
                                ad.isIsUsingLargeState(),
                                "should switch after reaching high threshold");
                        ad.add(row(Integer.toString(runningSize), "value"), 777L);
                    }
                    assertTrue(ad.isIsUsingLargeState());

                    state.clear();
                    assertFalse(
                            ad.isIsUsingLargeState(), "should switch to value state after clear");
                });
    }

    @TestTemplate
    public void testAddReturnValues() throws Exception {
        testReturnValues(SequencedMultiSetState::add);
    }

    @TestTemplate
    public void testAppendReturnValues() throws Exception {
        testReturnValues(SequencedMultiSetState::append);
    }

    private void testReturnValues(
            TriFunctionWithException<
                            SequencedMultiSetState<RowData>,
                            RowData,
                            Long,
                            StateChangeInfo<RowData>,
                            Exception>
                    updateFn)
            throws Exception {
        runTest(
                state -> {
                    StateChangeInfo<RowData> ret;

                    ret = updateFn.apply(state, row("key-1", "value"), 777L);
                    assertEquals(StateChangeType.ADDITION, ret.getChangeType());
                    assertEquals(1, ret.getSizeAfter());
                    assertTrue(ret.wasEmpty());

                    ret = updateFn.apply(state, row("key-2", "value"), 777L);
                    assertEquals(StateChangeType.ADDITION, ret.getChangeType());
                    assertEquals(2, ret.getSizeAfter());
                    assertFalse(ret.wasEmpty());

                    removeAndAssert(state, row("key-1"), REMOVAL_OTHER);
                    removeAndAssert(state, row("key-2"), REMOVAL_ALL, row("key-2"));

                    ret = updateFn.apply(state, row("key-3", "value"), 777L);
                    assertEquals(StateChangeType.ADDITION, ret.getChangeType());
                    assertEquals(1, ret.getSizeAfter());
                    assertTrue(ret.wasEmpty());
                });
    }

    private void runTest(ThrowingConsumer<SequencedMultiSetState<RowData>, Exception> test)
            throws Exception {
        runTest(
                (state, keyContext) -> {
                    keyContext.setCurrentKey("key1");
                    test.accept(state);
                });
    }

    private void runTest(
            BiConsumerWithException<
                            SequencedMultiSetState<RowData>, InternalKeyContext<String>, Exception>
                    test)
            throws Exception {
        runTest(test, new IdentityKeyExtractor(), KEY_POS);
    }

    private void runTest(
            BiConsumerWithException<
                            SequencedMultiSetState<RowData>, InternalKeyContext<String>, Exception>
                    test,
            KeyExtractor keyExtractor,
            int keyPos)
            throws Exception {
        SequencedMultiSetStateContext p =
                new SequencedMultiSetStateContext(
                        new RowDataSerializer(VARCHAR),
                        new MyGeneratedEqualiser(keyPos),
                        new MyGeneratedHashFunction(keyPos),
                        new RowDataSerializer(VARCHAR, VARCHAR),
                        keyExtractor,
                        new SequencedMultiSetStateConfig(
                                strategy,
                                adaptiveHighThresholdOverride,
                                adaptiveLowThresholdOverride,
                                StateTtlConfig.DISABLED,
                                TimeDomain.EVENT_TIME));

        MockEnvironment env = new MockEnvironmentBuilder().build();

        AbstractKeyedStateBackend<String> stateBackend =
                getKeyedStateBackend(env, StringSerializer.INSTANCE);

        RuntimeContext ctx =
                new StreamingRuntimeContext(
                        env,
                        Collections.emptyMap(),
                        UnregisteredMetricsGroup.createOperatorMetricGroup(),
                        new OperatorID(),
                        new TestProcessingTimeService(),
                        getKeyedStateStore(stateBackend),
                        ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES);

        test.accept(SequencedMultiSetState.create(p, ctx, "hashmap"), stateBackend);
    }

    private static KeyedStateStore getKeyedStateStore(KeyedStateBackend<String> stateBackend) {
        return new DefaultKeyedStateStore(
                stateBackend,
                new SerializerFactory() {
                    @Override
                    public <T> TypeSerializer<T> createSerializer(TypeInformation<T> ti) {
                        return ti.createSerializer(new SerializerConfigImpl());
                    }
                });
    }

    private static <T> AbstractKeyedStateBackend<T> getKeyedStateBackend(
            MockEnvironment env, TypeSerializer<T> keySerializer) throws IOException {
        String op = "test-operator";
        JobID jobId = new JobID();
        JobVertexID jobVertexId = new JobVertexID();
        KeyGroupRange emptyKeyGroupRange = KeyGroupRange.of(0, 10);
        int numberOfKeyGroups = emptyKeyGroupRange.getNumberOfKeyGroups();

        return new HashMapStateBackend()
                .createKeyedStateBackend(
                        new KeyedStateBackendParametersImpl<>(
                                env,
                                jobId,
                                op,
                                keySerializer,
                                numberOfKeyGroups,
                                emptyKeyGroupRange,
                                new KvStateRegistry().createTaskRegistry(jobId, jobVertexId),
                                TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(),
                                Collections.emptyList(),
                                new CloseableRegistry()));
    }

    private static class MyGeneratedEqualiser extends GeneratedRecordEqualiser {

        private final int keyPos;

        public MyGeneratedEqualiser(int keyPos) {
            super("", "", new Object[0]);
            this.keyPos = keyPos;
        }

        @Override
        public RecordEqualiser newInstance(ClassLoader classLoader) {
            return new TestRecordEqualiser(keyPos);
        }
    }

    private static class MyGeneratedHashFunction extends GeneratedHashFunction {

        private final int keyPos;

        public MyGeneratedHashFunction(int keyPos) {
            super("", "", new Object[0], new Configuration());
            this.keyPos = keyPos;
        }

        @Override
        public HashFunction newInstance(ClassLoader classLoader) {
            return new TestRecordEqualiser(keyPos);
        }
    }

    private static class TestRecordEqualiser implements RecordEqualiser, HashFunction {

        private final int keyPos;

        private TestRecordEqualiser(int keyPos) {
            this.keyPos = keyPos;
        }

        @Override
        public boolean equals(RowData row1, RowData row2) {
            return row1.getRowKind() == row2.getRowKind()
                    && row1.getString(keyPos).equals(row2.getString(keyPos));
        }

        @Override
        public int hashCode(Object data) {
            RowData rd = (RowData) data;
            return Objects.hash(rd.getRowKind(), rd.getString(keyPos));
        }
    }

    private static void assertStateContents(
            SequencedMultiSetState<RowData> state, RowData rowData, Long timestamp)
            throws Exception {
        assertStateContents(state, Tuple2.of(rowData, timestamp));
    }

    @SafeVarargs
    private static void assertStateContents(
            SequencedMultiSetState<RowData> state, Tuple2<RowData, Long>... expectedArr)
            throws Exception {
        List<Tuple2<RowData, Long>> actual = new ArrayList<>();
        state.iterator().forEachRemaining(actual::add);
        assertEquals(expectedArr.length == 0, state.isEmpty());
        assertEquals(expectedArr.length, actual.size());
        Assertions.assertArrayEquals(expectedArr, actual.toArray());
    }

    private static void removeAndAssert(
            SequencedMultiSetState<RowData> state,
            RowData key,
            StateChangeType expectedResultType,
            RowData... expectedReturnedRow)
            throws Exception {
        StateChangeInfo<RowData> ret = state.remove(key);

        assertEquals(expectedResultType, ret.getChangeType());
        switch (ret.getChangeType()) {
            case REMOVAL_NOT_FOUND:
                assertEquals(Optional.empty(), ret.getPayload());
                break;
            case REMOVAL_ALL:
                assertEquals(0, ret.getSizeAfter());
                assertTrue(state.isEmpty(), "state is expected to be empty");
                assertEquals(Optional.of(expectedReturnedRow[0]), ret.getPayload());
                break;
            case REMOVAL_OTHER:
                assertFalse(state.isEmpty(), "state is expected to be non-empty");
                assertEquals(Optional.empty(), ret.getPayload());
                break;
            case REMOVAL_LAST_ADDED:
                assertFalse(state.isEmpty(), "state is expected to be non-empty");
                assertEquals(Optional.of(expectedReturnedRow[0]), ret.getPayload());
                break;
        }
    }

    private static class IdentityKeyExtractor implements KeyExtractor {

        @Override
        public RowData apply(RowData rowData) {
            return rowData;
        }
    }
}
