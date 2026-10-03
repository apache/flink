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

package org.apache.flink.state.api.schema;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.StateTableUtils;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.api.utils.SavepointTestBase;
import org.apache.flink.state.catalog.StateCatalog;
import org.apache.flink.state.catalog.TuplePojoField;
import org.apache.flink.state.table.SavepointConnectorOptions;
import org.apache.flink.state.table.SavepointConnectorOptions.StateReaderMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests verifying that the savepoint/checkpoint table connector correctly exposes
 * non-keyed (operator) state — {@code ListState}, {@code UnionState}, and {@code BroadcastState} —
 * via the {@code _list}/{@code _union}/{@code _broadcast} tables.
 *
 * <p>Subclassed per state backend since non-keyed state storage is backend-agnostic but the
 * catalog/connector layer should still be guarded against backend-specific regressions.
 */
public abstract class StateCatalogNonKeyedITCase extends SavepointTestBase {

    protected abstract Configuration getConfiguration();

    private static final String UID = "operator-state-writer";
    private static final String LIST_STATE_NAME = "list-values";
    private static final String UNION_STATE_NAME = "union-values";
    private static final String BROADCAST_STATE_NAME = "broadcast-values";

    private static final String POJO_UID = "pojo-operator-state-writer";
    private static final String POJO_LIST_STATE_NAME = "pojo-list-values";
    private static final String POJO_UNION_STATE_NAME = "pojo-union-values";
    private static final String POJO_BROADCAST_STATE_NAME = "pojo-broadcast-values";

    /** A plain POJO (public fields, public no-arg constructor) used as an operator state value. */
    public static class NonKeyedEvent {
        public String name;
        public long value;

        public NonKeyedEvent() {}

        public NonKeyedEvent(String name, long value) {
            this.name = name;
            this.value = value;
        }
    }

    @Test
    public void testListUnionAndBroadcastStateExposedAsNonKeyedTables() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(getConfiguration());
        env.setParallelism(1);

        Integer[] data = new Integer[] {1, 2, 3};

        env.addSource(createSource(data))
                .map(new OperatorStateWriter())
                .uid(UID)
                .sinkTo(new DiscardingSink<>());

        String savepointPath = takeSavepoint(env);
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(savepointPath);
        OperatorIdentifier opId = OperatorIdentifier.forUid(UID);

        NonKeyedStateSchemaInfo schemaInfo = StateTableUtils.getNonKeyedStateSchema(metadata, opId);
        assertEquals(StateReaderMode.LIST, schemaInfo.stateSchemas.get(LIST_STATE_NAME).kind);
        NonKeyedStateSchemaInfo.StateEntryInfo unionEntry =
                schemaInfo.stateSchemas.get(UNION_STATE_NAME);
        assertEquals(StateReaderMode.UNION, unionEntry.kind);
        assertTrue(
                unionEntry.valueLogicalType instanceof RowType,
                "Expected the union state's value type to be a structured (ROW) type");
        NonKeyedStateSchemaInfo.StateEntryInfo broadcastEntry =
                schemaInfo.stateSchemas.get(BROADCAST_STATE_NAME);
        assertEquals(StateReaderMode.BROADCAST, broadcastEntry.kind);
        assertNotNull(
                broadcastEntry.mapKeyLogicalType,
                "Expected a resolved map key type for the broadcast state");

        TableEnvironment tableEnv = StateCatalogTestUtils.newTableEnv();
        StateCatalog catalog = StateCatalogTestUtils.registerCatalog(tableEnv, savepointPath);
        try {
            String dbName = catalog.listDatabases().get(0);
            tableEnv.useCatalog("state");
            tableEnv.useDatabase(dbName);

            String listTable =
                    StateCatalog.OPERATOR_UID_PREFIX
                            + UID
                            + "_"
                            + LIST_STATE_NAME
                            + StateCatalog.LIST_TABLE_SUFFIX;
            String unionTable =
                    StateCatalog.OPERATOR_UID_PREFIX
                            + UID
                            + "_"
                            + UNION_STATE_NAME
                            + StateCatalog.UNION_TABLE_SUFFIX;
            String broadcastTable =
                    StateCatalog.OPERATOR_UID_PREFIX
                            + UID
                            + "_"
                            + BROADCAST_STATE_NAME
                            + StateCatalog.BROADCAST_TABLE_SUFFIX;

            List<String> tables = catalog.listTables(dbName);
            assertTrue(tables.contains(listTable));
            assertTrue(tables.contains(unionTable));
            assertTrue(tables.contains(broadcastTable));

            // Non-keyed state is not stored in a state backend, so these tables must not advertise
            // a STATE_BACKEND_TYPE option (unlike the four keyed table kinds).
            for (String nonKeyedTable : Arrays.asList(listTable, unionTable, broadcastTable)) {
                CatalogTable catalogTable =
                        (CatalogTable) catalog.getTable(new ObjectPath(dbName, nonKeyedTable));
                assertFalse(
                        catalogTable
                                .getOptions()
                                .containsKey(SavepointConnectorOptions.STATE_BACKEND_TYPE.key()),
                        "Table '"
                                + nonKeyedTable
                                + "' should not have a STATE_BACKEND_TYPE option");
            }

            assertScalarListTable(tableEnv, listTable, LIST_STATE_NAME, Arrays.asList(10, 20, 30));
            assertUnionRowTable(
                    tableEnv,
                    unionTable,
                    Arrays.asList(new TuplePojoField("a", 100L), new TuplePojoField("b", 200L)));

            List<Row> broadcastRows =
                    StateCatalogTestUtils.collect(
                            tableEnv, "SELECT * FROM `" + broadcastTable + "` ORDER BY map_key");
            assertEquals(2, broadcastRows.size());
            assertEquals(1, broadcastRows.get(0).getField("map_key"));
            assertEquals("one", broadcastRows.get(0).getField("map_value"));
            assertEquals(2, broadcastRows.get(1).getField("map_key"));
            assertEquals("two", broadcastRows.get(1).getField("map_value"));
        } finally {
            catalog.close();
        }
    }

    // Verifies the generic PojoToRowDataDeserializer fallback: reads must succeed even with the
    // POJO's class hidden from the classpath (simulated via a custom context classloader), instead
    // of throwing ClassNotFoundException.
    @Test
    public void testListUnionAndBroadcastStateWithMissingPojoClassFallsBackToRowData()
            throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(getConfiguration());
        env.setParallelism(1);

        Integer[] data = new Integer[] {1, 2, 3};

        env.addSource(createSource(data))
                .map(new PojoOperatorStateWriter())
                .uid(POJO_UID)
                .sinkTo(new DiscardingSink<>());

        String savepointPath = takeSavepoint(env);

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader hidingEventClass =
                new ClassLoader(original) {
                    @Override
                    public Class<?> loadClass(String name) throws ClassNotFoundException {
                        if (name.equals(NonKeyedEvent.class.getName())) {
                            throw new ClassNotFoundException(name);
                        }
                        return super.loadClass(name);
                    }
                };
        Thread.currentThread().setContextClassLoader(hidingEventClass);
        try {
            TableEnvironment tableEnv = StateCatalogTestUtils.newTableEnv();
            StateCatalog catalog = StateCatalogTestUtils.registerCatalog(tableEnv, savepointPath);
            try {
                String dbName = catalog.listDatabases().get(0);
                tableEnv.useCatalog("state");
                tableEnv.useDatabase(dbName);

                String listTable =
                        StateCatalog.OPERATOR_UID_PREFIX
                                + POJO_UID
                                + "_"
                                + POJO_LIST_STATE_NAME
                                + StateCatalog.LIST_TABLE_SUFFIX;
                String unionTable =
                        StateCatalog.OPERATOR_UID_PREFIX
                                + POJO_UID
                                + "_"
                                + POJO_UNION_STATE_NAME
                                + StateCatalog.UNION_TABLE_SUFFIX;
                String broadcastTable =
                        StateCatalog.OPERATOR_UID_PREFIX
                                + POJO_UID
                                + "_"
                                + POJO_BROADCAST_STATE_NAME
                                + StateCatalog.BROADCAST_TABLE_SUFFIX;

                List<Row> listRows =
                        StateCatalogTestUtils.collect(
                                tableEnv, "SELECT * FROM `" + listTable + "` ORDER BY `value`");
                assertEquals(2, listRows.size());
                assertEquals("shared", listRows.get(0).getField("name"));
                assertEquals(10L, listRows.get(0).getField("value"));
                assertEquals("shared", listRows.get(1).getField("name"));
                assertEquals(20L, listRows.get(1).getField("value"));

                List<Row> unionRows =
                        StateCatalogTestUtils.collect(
                                tableEnv, "SELECT * FROM `" + unionTable + "` ORDER BY `value`");
                assertEquals(2, unionRows.size());
                assertEquals("shared", unionRows.get(0).getField("name"));
                assertEquals(10L, unionRows.get(0).getField("value"));
                assertEquals("shared", unionRows.get(1).getField("name"));
                assertEquals(20L, unionRows.get(1).getField("value"));

                List<Row> broadcastRows =
                        StateCatalogTestUtils.collect(
                                tableEnv,
                                "SELECT * FROM `" + broadcastTable + "` ORDER BY map_key");
                assertEquals(2, broadcastRows.size());
                assertEquals(1, broadcastRows.get(0).getField("map_key"));
                Row firstValue = broadcastRows.get(0).getFieldAs("map_value");
                assertEquals("one", firstValue.getField("name"));
                assertEquals(100L, firstValue.getField("value"));
                assertEquals(2, broadcastRows.get(1).getField("map_key"));
                Row secondValue = broadcastRows.get(1).getFieldAs("map_value");
                assertEquals("two", secondValue.getField("name"));
                assertEquals(200L, secondValue.getField("value"));
            } finally {
                catalog.close();
            }
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Asserts a scalar-valued {@code _list} table has one column matching {@code expectedValues}.
     */
    private static void assertScalarListTable(
            TableEnvironment tableEnv,
            String tableName,
            String valueColumn,
            List<Integer> expectedValues)
            throws Exception {
        Table table = tableEnv.sqlQuery("SELECT * FROM `" + tableName + "`");
        assertEquals(
                Collections.singletonList(valueColumn), table.getResolvedSchema().getColumnNames());

        List<Row> rows =
                StateCatalogTestUtils.collect(tableEnv, "SELECT * FROM `" + tableName + "`");
        assertEquals(expectedValues.size(), rows.size());

        List<Integer> values = new ArrayList<>();
        for (Row row : rows) {
            values.add(row.getFieldAs(valueColumn));
        }
        assertEquals(new HashSet<>(expectedValues), new HashSet<>(values));
    }

    /**
     * Asserts a structured (ROW-valued) {@code _union} table is flattened into {@code (name,
     * score)} columns with no wrapping value column, matching {@code expectedValues}.
     */
    private static void assertUnionRowTable(
            TableEnvironment tableEnv, String tableName, List<TuplePojoField> expectedValues)
            throws Exception {
        Table table = tableEnv.sqlQuery("SELECT * FROM `" + tableName + "`");
        assertEquals(Arrays.asList("name", "score"), table.getResolvedSchema().getColumnNames());

        List<Row> rows =
                StateCatalogTestUtils.collect(tableEnv, "SELECT * FROM `" + tableName + "`");
        assertEquals(expectedValues.size(), rows.size());

        List<TuplePojoField> values = new ArrayList<>();
        for (Row row : rows) {
            values.add(
                    new TuplePojoField(
                            row.<String>getFieldAs("name"), row.<Long>getFieldAs("score")));
        }
        assertEquals(new HashSet<>(expectedValues), new HashSet<>(values));
    }

    /**
     * Registers a {@code ListState}, {@code UnionState}, and {@code BroadcastState} with fixed test
     * data.
     */
    private static class OperatorStateWriter extends RichMapFunction<Integer, Integer>
            implements CheckpointedFunction {

        private transient ListState<Integer> listState;
        private transient ListState<TuplePojoField> unionState;
        private transient BroadcastState<Integer, String> broadcastState;

        @Override
        public Integer map(Integer value) {
            return value;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(LIST_STATE_NAME, Integer.class));
            unionState =
                    context.getOperatorStateStore()
                            .getUnionListState(
                                    new ListStateDescriptor<>(
                                            UNION_STATE_NAME, TuplePojoField.class));
            broadcastState =
                    context.getOperatorStateStore()
                            .getBroadcastState(
                                    new MapStateDescriptor<>(
                                            BROADCAST_STATE_NAME, Integer.class, String.class));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.update(Arrays.asList(10, 20, 30));
            unionState.update(
                    Arrays.asList(new TuplePojoField("a", 100L), new TuplePojoField("b", 200L)));
            broadcastState.put(1, "one");
            broadcastState.put(2, "two");
        }
    }

    /**
     * Like {@link OperatorStateWriter}, but with a POJO value type so classpath-fallback reads can
     * be exercised.
     */
    private static class PojoOperatorStateWriter extends RichMapFunction<Integer, Integer>
            implements CheckpointedFunction {

        private transient ListState<NonKeyedEvent> listState;
        private transient ListState<NonKeyedEvent> unionState;
        private transient BroadcastState<Integer, NonKeyedEvent> broadcastState;

        @Override
        public Integer map(Integer value) {
            return value;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            POJO_LIST_STATE_NAME, NonKeyedEvent.class));
            unionState =
                    context.getOperatorStateStore()
                            .getUnionListState(
                                    new ListStateDescriptor<>(
                                            POJO_UNION_STATE_NAME, NonKeyedEvent.class));
            broadcastState =
                    context.getOperatorStateStore()
                            .getBroadcastState(
                                    new MapStateDescriptor<>(
                                            POJO_BROADCAST_STATE_NAME,
                                            Integer.class,
                                            NonKeyedEvent.class));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.update(
                    Arrays.asList(
                            new NonKeyedEvent("shared", 10L), new NonKeyedEvent("shared", 20L)));
            unionState.update(
                    Arrays.asList(
                            new NonKeyedEvent("shared", 10L), new NonKeyedEvent("shared", 20L)));
            broadcastState.put(1, new NonKeyedEvent("one", 100L));
            broadcastState.put(2, new NonKeyedEvent("two", 200L));
        }
    }
}
