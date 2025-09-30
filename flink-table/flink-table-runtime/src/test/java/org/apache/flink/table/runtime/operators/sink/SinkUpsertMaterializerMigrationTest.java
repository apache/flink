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

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.FlinkVersion;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MigrationTest;
import org.apache.flink.types.RowKind;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.flink.FlinkVersion.current;
import static org.apache.flink.streaming.util.OperatorSnapshotUtil.getResourceFilename;
import static org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializerTest.ASSERTOR;
import static org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializerTest.EQUALISER;
import static org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializerTest.LOGICAL_TYPES;
import static org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializerTest.TTL_CONFIG;
import static org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializerTest.UPSERT_KEY;
import static org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializerTest.UPSERT_KEY_EQUALISER;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.rowOfKind;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;

/** Test for {@link SinkUpsertMaterializer} migration. */
@RunWith(Parameterized.class)
public class SinkUpsertMaterializerMigrationTest implements MigrationTest {

    private static final String FOLDER_NAME = "sink-upsert-materializer";

    @Parameterized.Parameter(0)
    @SuppressWarnings({"ClassEscapesDefinedScope", "DefaultAnnotationParam"})
    public SinkOperationMode migrateFrom;

    @Parameterized.Parameter(1)
    @SuppressWarnings("ClassEscapesDefinedScope")
    public SinkOperationMode migrateTo;

    @Parameterized.Parameters(name = "{0} -> {1}")
    public static List<Object[]> parameters() {
        List<Object[]> result = new ArrayList<>();
        Set<FlinkVersion> versions = FlinkVersion.rangeOf(FlinkVersion.v2_2, FlinkVersion.v2_2);
        for (FlinkVersion fromVersion : versions) {
            for (SinkUpsertMaterializerStateBackend backend :
                    SinkUpsertMaterializerStateBackend.values()) {
                result.add(
                        new Object[] {
                            new SinkOperationMode(fromVersion, backend),
                            new SinkOperationMode(current(), backend)
                        });
            }
        }
        return result;
    }

    @Test
    public void testMigration() throws Exception {
        String path = getResourceFilename(FOLDER_NAME + "/" + getFileName(migrateFrom));
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createHarness(migrateTo, path)) {
            testCorrectnessAfterSnapshot(harness);
        }
    }

    private OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
            SinkOperationMode mode, String snapshotPath) throws Exception {
        int[] inputUpsertKey = {UPSERT_KEY};
        OneInputStreamOperator<RowData, RowData> materializer =
                SinkUpsertMaterializer.create(
                        TTL_CONFIG,
                        RowType.of(LOGICAL_TYPES),
                        EQUALISER,
                        UPSERT_KEY_EQUALISER,
                        inputUpsertKey);
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                SinkUpsertMaterializerTest.createHarness(
                        materializer, mode.stateBackend, LOGICAL_TYPES);
        harness.setup(new RowDataSerializer(LOGICAL_TYPES));
        if (snapshotPath != null) {
            harness.initializeState(snapshotPath);
        }
        harness.open();
        harness.setStateTtlProcessingTime(1);
        return harness;
    }

    private void testCorrectnessBeforeSnapshot(
            OneInputStreamOperatorTestHarness<RowData, RowData> testHarness) throws Exception {

        testHarness.processElement(insertRecord(1L, 1, "a1"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.INSERT, 1L, 1, "a1"));

        testHarness.processElement(updateAfterRecord(1L, 1, "a11"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.UPDATE_AFTER, 1L, 1, "a11"));

        testHarness.processElement(insertRecord(3L, 1, "a3"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.UPDATE_AFTER, 3L, 1, "a3"));
    }

    private void testCorrectnessAfterSnapshot(
            OneInputStreamOperatorTestHarness<RowData, RowData> testHarness) throws Exception {
        testHarness.processElement(deleteRecord(1L, 1, "a111"));
        ASSERTOR.shouldEmitNothing(testHarness);

        testHarness.processElement(deleteRecord(3L, 1, "a33"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.DELETE, 3L, 1, "a33"));

        testHarness.processElement(insertRecord(4L, 1, "a4"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.INSERT, 4L, 1, "a4"));

        testHarness.setStateTtlProcessingTime(1002);
        testHarness.processElement(deleteRecord(4L, 1, "a4"));
        ASSERTOR.shouldEmitNothing(testHarness);
    }

    private static String getFileName(SinkOperationMode mode) {
        return String.format(
                "migration-flink-%s-%s-%s-snapshot", mode.version, mode.stateBackend, "V1");
    }

    @SnapshotsGenerator
    public void writeSnapshot(FlinkVersion version) throws Exception {
        for (SinkUpsertMaterializerStateBackend stateBackend :
                SinkUpsertMaterializerStateBackend.values()) {
            SinkOperationMode mode = new SinkOperationMode(version, stateBackend);
            try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                    createHarness(mode, null)) {
                testCorrectnessBeforeSnapshot(harness);
                Path parent = Paths.get("src/test/resources", FOLDER_NAME);
                Files.createDirectories(parent);
                OperatorSnapshotUtil.writeStateHandle(
                        harness.snapshot(1L, 1L), parent.resolve(getFileName(mode)).toString());
            }
        }
    }

    public static void main(String... s) throws Exception {
        // Run this to manually generate snapshot files for migration tests
        // set working directory to flink-table/flink-table-runtime/
        new SinkUpsertMaterializerMigrationTest().writeSnapshot(current());
    }

    private static class SinkOperationMode {
        private final FlinkVersion version;
        private final SinkUpsertMaterializerStateBackend stateBackend;

        private SinkOperationMode(
                FlinkVersion version, SinkUpsertMaterializerStateBackend stateBackend) {
            this.version = version;
            this.stateBackend = stateBackend;
        }

        @Override
        public String toString() {
            return String.format("flink=%s, state=%s}", version, stateBackend);
        }
    }
}
