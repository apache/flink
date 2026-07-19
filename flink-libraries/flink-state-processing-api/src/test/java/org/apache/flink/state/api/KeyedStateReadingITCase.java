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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.api.schema.KeyedStateSchemaInfo;
import org.apache.flink.state.api.utils.SavepointTestBase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration tests that write real keyed state through a MiniCluster job, take a savepoint at
 * runtime, and read it back — verified against both the heap ({@code hashmap}) and RocksDB state
 * backends (see {@code HashMapKeyedStateReadingITCase} / {@code
 * EmbeddedRocksDBKeyedStateReadingITCase}) so that schema extraction and reads are checked against
 * both keyed-state-handle formats.
 */
public abstract class KeyedStateReadingITCase extends SavepointTestBase {

    protected abstract Configuration getConfiguration();

    // -------------------------------------------------------------------------
    // Schema extraction: RowData-typed internal SQL operator state
    // -------------------------------------------------------------------------

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
}
