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

package org.apache.flink.state.api;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializerSnapshot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.state.api.input.deserializer.PojoToRowDataDeserializer;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.api.schema.KeyedStateSchemaInfo;
import org.apache.flink.state.api.schema.StateSchemaExtractor;
import org.apache.flink.state.api.schema.StateSchemaInfo;
import org.apache.flink.state.api.utils.SavepointTestBase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests that write real keyed state through a MiniCluster job, take a savepoint at
 * runtime, and read it back — verified against both the heap ({@code hashmap}) and RocksDB state
 * backends (see {@code HashMapKeyedStateReadingITCase} / {@code
 * EmbeddedRocksDBKeyedStateReadingITCase}) so that schema extraction and reads are checked against
 * both keyed-state-handle formats.
 */
public abstract class KeyedStateReadingITCase extends SavepointTestBase {

    protected abstract Configuration getConfiguration();

    /** Deliberately simple so no Kryo or special serializers are needed. */
    public static class PersonPojo {
        public String name;
        public int age;
        public long score;

        public PersonPojo() {}

        public PersonPojo(String name, int age, long score) {
            this.name = name;
            this.age = age;
            this.score = score;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PersonPojo)) {
                return false;
            }
            PersonPojo other = (PersonPojo) o;
            return Objects.equals(name, other.name) && age == other.age && score == other.score;
        }
    }

    // -------------------------------------------------------------------------
    // Schema extraction: RowData-typed internal SQL operator state
    // -------------------------------------------------------------------------

    private static final ValueStateDescriptor<PersonPojo> PERSON_STATE_DESC =
            new ValueStateDescriptor<>("person", PersonPojo.class);

    @Test
    public void testGroupAggAccStateSchemaExtraction() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(getConfiguration());
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Tuple2<String, Long>[] data =
                new Tuple2[] {
                    Tuple2.of("a", 1L), Tuple2.of("a", 2L), Tuple2.of("b", 3L),
                };
        DataStream<Tuple2<String, Long>> source = env.addSource(createSource(data));
        tEnv.createTemporaryView(
                "t",
                source,
                Schema.newBuilder().column("f0", "STRING").column("f1", "BIGINT").build());

        Table result =
                tEnv.sqlQuery(
                        "SELECT f0 AS `key`, COUNT(*) AS cnt, SUM(f1) AS total FROM t GROUP BY f0");

        DataStream<Row> resultStream = tEnv.toChangelogStream(result);
        resultStream.sinkTo(new DiscardingSink<>());

        String savepointPath = takeSavepoint(env);
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(savepointPath);

        // SQL-planned operators don't carry a user-assigned uid; find the aggregation operator by
        // the state it registers.
        OperatorIdentifier aggOpId = null;
        for (OperatorIdentifier candidate : StateTableUtils.getOperatorIdentifiers(metadata)) {
            List<String> stateNames = StateTableUtils.getKeyedStates(metadata, candidate);
            if (stateNames.contains("accState")) {
                aggOpId = candidate;
                break;
            }
        }
        assertNotNull(aggOpId, "Could not find operator with 'accState'");

        KeyedStateSchemaInfo schemaInfo = StateTableUtils.getKeyedStateSchema(metadata, aggOpId);
        KeyedStateSchemaInfo.StateEntryInfo accEntry = schemaInfo.stateSchemas.get("accState");
        assertNotNull(accEntry, "'accState' not found in extracted schema");

        assertEquals(LogicalTypeRoot.ROW, accEntry.logicalType.getTypeRoot());
        RowType rowType = (RowType) accEntry.logicalType;

        // The accumulator row holds one field per aggregate call: COUNT(*) and SUM(f1).
        assertEquals(2, rowType.getFieldCount());
        assertEquals(LogicalTypeRoot.BIGINT, rowType.getFields().get(0).getType().getTypeRoot());
        assertEquals(LogicalTypeRoot.BIGINT, rowType.getFields().get(1).getType().getTypeRoot());
    }

    // -------------------------------------------------------------------------
    // Schema extraction: POJO value state
    // -------------------------------------------------------------------------

    private static final String POJO_UID = "pojo-state-operator";

    @Test
    public void testSchemaExtractionFromPojoState() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(getConfiguration());
        env.setParallelism(1);

        PersonPojo[] data = {
            new PersonPojo("Alice", 30, 100L),
            new PersonPojo("Bob", 25, 200L),
            new PersonPojo("Carol", 35, 300L)
        };
        env.addSource(createSource(data))
                .returns(PersonPojo.class)
                .keyBy(p -> p.name)
                .process(new PersonStateWriter())
                .uid(POJO_UID)
                .sinkTo(new DiscardingSink<>());

        String savepointPath = takeSavepoint(env);
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(savepointPath);

        OperatorIdentifier opId = OperatorIdentifier.forUid(POJO_UID);

        // Discover states via StateTableUtils
        List<String> stateNames = StateTableUtils.getKeyedStates(metadata, opId);
        assertTrue(stateNames.contains("person"), "Expected 'person' state");

        // Extract schema via StateTableUtils — all states in one call
        KeyedStateSchemaInfo schemaInfo = StateTableUtils.getKeyedStateSchema(metadata, opId);
        KeyedStateSchemaInfo.StateEntryInfo personEntry = schemaInfo.stateSchemas.get("person");
        assertNotNull(personEntry, "'person' state not found in schema");

        // PersonPojo has 3 fields → the logicalType should be a RowType with 3 fields
        assertEquals(LogicalTypeRoot.ROW, personEntry.logicalType.getTypeRoot());
        RowType rowType = (RowType) personEntry.logicalType;
        assertEquals(3, rowType.getFieldCount());
        assertHasField(rowType, "name", LogicalTypeRoot.VARCHAR);
        assertHasField(rowType, "age", LogicalTypeRoot.INTEGER);
        assertHasField(rowType, "score", LogicalTypeRoot.BIGINT);
    }

    @Test
    public void testDeserializerBuiltFromPojoSnapshot() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(getConfiguration());
        env.setParallelism(1);

        PersonPojo[] data = {new PersonPojo("Alice", 30, 100L)};
        env.addSource(createSource(data))
                .returns(PersonPojo.class)
                .keyBy(p -> p.name)
                .process(new PersonStateWriter())
                .uid(POJO_UID)
                .sinkTo(new DiscardingSink<>());

        String savepointPath = takeSavepoint(env);
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(savepointPath);

        OperatorIdentifier opId = OperatorIdentifier.forUid(POJO_UID);

        KeyedStateSchemaInfo schemaInfo = StateTableUtils.getKeyedStateSchema(metadata, opId);
        KeyedStateSchemaInfo.StateEntryInfo personEntry = schemaInfo.stateSchemas.get("person");
        assertNotNull(personEntry);

        // Building the PojoToRowDataDeserializer directly from the snapshot (lower-level API)
        List<StateSchemaInfo> rawSchemas =
                StateSchemaExtractor.extractSchema(findOperatorState(metadata, opId));
        StateSchemaInfo personRaw =
                rawSchemas.stream()
                        .filter(s -> "person".equals(s.stateName))
                        .findFirst()
                        .orElse(null);
        assertNotNull(personRaw);

        var deser =
                PojoToRowDataDeserializer.create(
                        (PojoSerializerSnapshot<?>) personRaw.valueSnapshot);
        assertNotNull(deser);
        assertTrue(
                deser instanceof PojoToRowDataDeserializer,
                "Expected PojoToRowDataDeserializer, got: " + deser.getClass().getSimpleName());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static OperatorState findOperatorState(
            CheckpointMetadata metadata, OperatorIdentifier opId) {
        for (OperatorState op : metadata.getOperatorStates()) {
            if (op.getOperatorID().equals(opId.getOperatorId())) {
                return op;
            }
        }
        throw new IllegalArgumentException("Operator not found: " + opId);
    }

    private static void assertHasField(RowType row, String name, LogicalTypeRoot expectedRoot) {
        RowType.RowField field =
                row.getFields().stream()
                        .filter(f -> f.getName().equals(name))
                        .findFirst()
                        .orElse(null);
        assertNotNull(field, "Field '" + name + "' not found in row type");
        assertEquals(
                expectedRoot, field.getType().getTypeRoot(), "Wrong type for field '" + name + "'");
    }

    // -------------------------------------------------------------------------
    // Operators
    // -------------------------------------------------------------------------

    private static class PersonStateWriter extends KeyedProcessFunction<String, PersonPojo, Void> {
        private transient ValueState<PersonPojo> state;

        @Override
        public void open(OpenContext ctx) throws Exception {
            state = getRuntimeContext().getState(PERSON_STATE_DESC);
        }

        @Override
        public void processElement(PersonPojo value, Context ctx, Collector<Void> out)
                throws Exception {
            state.update(value);
        }
    }
}
