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
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.RemovalResultType;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.SizeChangeInfo;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.Strategy;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.RemovalResultType.ALL_REMOVED;
import static org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.RemovalResultType.NOTHING_REMOVED;
import static org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.RemovalResultType.REMOVED_LAST_ADDED;
import static org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.RemovalResultType.REMOVED_OTHER;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
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
                    assertTrue(state.isEmpty());
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

                    state.add(row("k1", "y"), 778L);
                    assertStateContents(state, row("k1", "y"), 778L);

                    state.add(row("k2", "y"), 778L);
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
                    removeAndAssert(state, row("key1"), NOTHING_REMOVED);

                    state.add(row("key1", "value"), 777L);
                    state.add(row("key2", "value"), 777L);
                    state.add(row("key3", "value"), 777L);
                    state.add(row("key4", "value"), 777L);

                    removeAndAssert(state, row("key999"), NOTHING_REMOVED);
                    removeAndAssert(state, row("key4"), REMOVED_LAST_ADDED, row("key3", "value"));
                    removeAndAssert(state, row("key3"), REMOVED_LAST_ADDED, row("key2", "value"));
                    removeAndAssert(state, row("key1"), REMOVED_OTHER);
                    removeAndAssert(state, row("key2"), ALL_REMOVED, row("key2", "value"));
                });
    }

    @TestTemplate
    public void testRemoveFirstAppended() throws Exception {
        runTest(
                state -> {
                    state.append(row("key", "value-1"), 777L);
                    state.append(row("key", "value-2"), 778L);
                    state.append(row("key", "value-3"), 779L);

                    removeAndAssert(state, row("key"), REMOVED_OTHER);
                    assertStateContents(
                            state,
                            Tuple2.of(row("key", "value-2"), 778L),
                            Tuple2.of(row("key", "value-3"), 779L));

                    removeAndAssert(state, row("key"), REMOVED_OTHER);
                    assertStateContents(state, Tuple2.of(row("key", "value-3"), 779L));

                    removeAndAssert(state, row("key"), ALL_REMOVED, row("key", "value-3"));
                    assertTrue(state.isEmpty());
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

    @TestTemplate
    public void testAdaptivity() throws Exception {
        assumeTrue(strategy == Strategy.ADAPTIVE);
        final long totalSize = adaptiveHighThresholdOverride * 2;
        runTest(
                (state, ctx) -> {
                    AdaptiveSequencedMultiSetState ad = (AdaptiveSequencedMultiSetState) state;

                    ctx.setCurrentKey("k1");
                    assertFalse(ad.isIsUsingLargeState(), "should start with value state");
                    for (int size = 0; size < totalSize; size++) {
                        assertEquals(
                                size >= adaptiveHighThresholdOverride,
                                ad.isIsUsingLargeState(),
                                "should switch after reaching high threshold");
                        ad.append(row("key", "value"), 777L);
                    }

                    ctx.setCurrentKey("k2");
                    assertFalse(ad.isIsUsingLargeState(), "should not mix different context keys");

                    ctx.setCurrentKey("k1");
                    assertTrue(ad.isIsUsingLargeState(), "should not mix different context keys");

                    for (long size = totalSize; size >= 0; size--) {
                        assertEquals(
                                size > adaptiveLowThresholdOverride,
                                ad.isIsUsingLargeState(),
                                "should switch back after reaching low threshold");
                        ad.remove(row("key"));
                    }

                    for (int size = 0; size < totalSize; size++) {
                        ad.append(row("key", "value"), 777L);
                    }
                    assertTrue(ad.isIsUsingLargeState());

                    state.clear();
                    assertFalse(
                            ad.isIsUsingLargeState(), "should switch to value state after clear");
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
        SequencedMultiSetStateContext p =
                new SequencedMultiSetStateContext(
                        new RowDataSerializer(VARCHAR),
                        new MyGeneratedEqualiser(),
                        new MyGeneratedHashFunction(),
                        new RowDataSerializer(VARCHAR, VARCHAR),
                        Function.identity(),
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

        public MyGeneratedEqualiser() {
            super("", "", new Object[0]);
        }

        @Override
        public RecordEqualiser newInstance(ClassLoader classLoader) {
            return new TestRecordEqualiser();
        }
    }

    private static class MyGeneratedHashFunction extends GeneratedHashFunction {

        public MyGeneratedHashFunction() {
            super("", "", new Object[0], new Configuration());
        }

        @Override
        public HashFunction newInstance(ClassLoader classLoader) {
            return new TestRecordEqualiser();
        }
    }

    private static class TestRecordEqualiser implements RecordEqualiser, HashFunction {
        @Override
        public boolean equals(RowData row1, RowData row2) {
            return row1.getRowKind() == row2.getRowKind()
                    && row1.getString(KEY_POS).equals(row2.getString(KEY_POS));
        }

        @Override
        public int hashCode(Object data) {
            RowData rd = (RowData) data;
            return Objects.hash(rd.getRowKind(), rd.getString(KEY_POS));
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
            RemovalResultType expectedResultType,
            @Nullable RowData... expectedReturnedRow)
            throws Exception {
        Tuple3<RemovalResultType, Optional<RowData>, SizeChangeInfo> ret = state.remove(key);

        RemovalResultType resultType = ret.f0;
        Optional<RowData> rowData = ret.f1;
        SizeChangeInfo sizeChangeInfo = ret.f2;

        assertEquals(expectedResultType, resultType);
        switch (resultType) {
            case NOTHING_REMOVED:
                assertEquals(sizeChangeInfo.sizeBefore, sizeChangeInfo.sizeAfter);
                assertEquals(Optional.empty(), rowData);
                break;
            case ALL_REMOVED:
                assertEquals(0, sizeChangeInfo.sizeAfter);
                assertTrue(state.isEmpty(), "state is expected to be empty");
                assertEquals(Optional.of(expectedReturnedRow[0]), rowData);
                break;
            case REMOVED_OTHER:
                assertEquals(sizeChangeInfo.sizeBefore - 1, sizeChangeInfo.sizeAfter);
                assertFalse(state.isEmpty(), "state is expected to be non-empty");
                assertEquals(Optional.empty(), rowData);
                break;
            case REMOVED_LAST_ADDED:
                assertEquals(sizeChangeInfo.sizeBefore - 1, sizeChangeInfo.sizeAfter);
                assertFalse(state.isEmpty(), "state is expected to be non-empty");
                assertEquals(Optional.of(expectedReturnedRow[0]), rowData);
                break;
        }
    }
}
