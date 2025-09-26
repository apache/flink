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

package org.apache.flink.state.rocksdb;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.state.rocksdb.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;

import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to verify that auto-compaction is correctly configured during RocksDB incremental restore
 * with ingest DB mode. This test ensures that production DBs maintain auto-compaction enabled while
 * temporary DBs used during restore have auto-compaction disabled for performance.
 */
public class RocksDBAutoCompactionIngestRestoreTest {

    @TempDir private java.nio.file.Path tempFolder;

    private static final int MAX_PARALLELISM = 10;

    @Test
    public void testAutoCompactionEnabledWithIngestDBRestore() throws Exception {
        // Create two subtask snapshots and merge them to trigger the multi-state-handle scenario
        // required for reproducing the ingest DB restore path
        OperatorSubtaskState operatorSubtaskState =
                AbstractStreamOperatorTestHarness.repackageState(
                        createSubtaskSnapshot(0), createSubtaskSnapshot(1));

        OperatorSubtaskState initState =
                AbstractStreamOperatorTestHarness.repartitionOperatorState(
                        operatorSubtaskState, MAX_PARALLELISM, 2, 1, 0);

        // Restore with ingest DB mode and verify auto-compaction
        try (KeyedOneInputStreamOperatorTestHarness<String, Tuple2<String, String>, String>
                harness = createTestHarness(new TestKeyedFunction(), MAX_PARALLELISM, 1, 0)) {

            EmbeddedRocksDBStateBackend stateBackend = createStateBackend(true);
            harness.setStateBackend(stateBackend);
            harness.setCheckpointStorage(
                    new FileSystemCheckpointStorage(
                            "file://" + tempFolder.resolve("checkpoint-restore").toAbsolutePath()));

            harness.initializeState(initState);
            harness.open();

            verifyAutoCompactionEnabled(harness);
        }
    }

    private OperatorSubtaskState createSubtaskSnapshot(int subtaskIndex) throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, Tuple2<String, String>, String>
                harness =
                        createTestHarness(
                                new TestKeyedFunction(), MAX_PARALLELISM, 2, subtaskIndex)) {

            harness.setStateBackend(createStateBackend(false));
            harness.setCheckpointStorage(
                    new FileSystemCheckpointStorage(
                            "file://"
                                    + tempFolder
                                            .resolve("checkpoint-subtask" + subtaskIndex)
                                            .toAbsolutePath()));
            harness.open();

            // Create an empty snapshot - data content doesn't matter for this test
            return harness.snapshot(0, 0);
        }
    }

    private void verifyAutoCompactionEnabled(
            KeyedOneInputStreamOperatorTestHarness<String, Tuple2<String, String>, String> harness)
            throws Exception {
        KeyedStateBackend<String> backend = harness.getOperator().getKeyedStateBackend();
        assertThat(backend).isNotNull();

        LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation =
                ((RocksDBKeyedStateBackend<String>) backend).getKvStateInformation();

        assertThat(kvStateInformation).as("kvStateInformation should not be empty").isNotEmpty();

        for (RocksDbKvStateInfo stateInfo : kvStateInformation.values()) {
            ColumnFamilyHandle handle = stateInfo.columnFamilyHandle;
            assertThat(handle).isNotNull();

            ColumnFamilyDescriptor descriptor = handle.getDescriptor();
            ColumnFamilyOptions options = descriptor.getOptions();

            assertThat(options.disableAutoCompactions())
                    .as(
                            "Production DB should have auto-compaction enabled for column family: "
                                    + stateInfo.metaInfo.getName())
                    .isFalse();
        }
    }

    private KeyedOneInputStreamOperatorTestHarness<String, Tuple2<String, String>, String>
            createTestHarness(
                    TestKeyedFunction keyedFunction,
                    int maxParallelism,
                    int parallelism,
                    int subtaskIndex)
                    throws Exception {

        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(keyedFunction),
                tuple2 -> tuple2.f0,
                BasicTypeInfo.STRING_TYPE_INFO,
                maxParallelism,
                parallelism,
                subtaskIndex);
    }

    private EmbeddedRocksDBStateBackend createStateBackend(boolean useIngestDbRestoreMode) {
        Configuration config = new Configuration();
        config.set(RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE, useIngestDbRestoreMode);

        EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(true);
        return stateBackend.configure(config, getClass().getClassLoader());
    }

    private static class TestKeyedFunction
            extends KeyedProcessFunction<String, Tuple2<String, String>, String> {
        private ValueState<String> state;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            state =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("test-state", String.class));
        }

        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out)
                throws Exception {
            state.update(value.f1);
            out.collect(value.f1);
        }
    }
}
