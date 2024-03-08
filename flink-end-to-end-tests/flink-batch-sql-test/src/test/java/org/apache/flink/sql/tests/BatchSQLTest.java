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

package org.apache.flink.sql.tests;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.IteratorInputFormat;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(TestLoggerExtension.class)
class BatchSQLTest {
    private static final Logger LOG = LoggerFactory.getLogger(BatchSQLTest.class);

    private static final Path sqlPath =
            ResourceTestUtils.getResource("resources/sql-job-query.sql");

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @ParameterizedTest
    @EnumSource(
            value = BatchShuffleMode.class,
            names = {
                "ALL_EXCHANGES_BLOCKING",
                "ALL_EXCHANGES_HYBRID_FULL",
                "ALL_EXCHANGES_HYBRID_SELECTIVE"
            })
    public void testBatchSQL(BatchShuffleMode shuffleMode, @TempDir Path tmpDir) throws Exception {
        final Path resultFile = tmpDir.resolve(String.format("result-%s", UUID.randomUUID()));
        LOG.info("Results for this test will be stored at: {}", resultFile);

        String sqlStatement = new String(Files.readAllBytes(sqlPath));

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set(ExecutionOptions.BATCH_SHUFFLE_MODE, shuffleMode);

        ((TableEnvironmentInternal) tEnv)
                .registerTableSourceInternal("table1", new GeneratorTableSource(10, 100, 60, 0));
        ((TableEnvironmentInternal) tEnv)
                .registerTableSourceInternal("table2", new GeneratorTableSource(5, 0.2f, 60, 5));
        ((TableEnvironmentInternal) tEnv)
                .registerTableSinkInternal(
                        "sinkTable",
                        new CsvTableSink(resultFile.toString())
                                .configure(
                                        new String[] {"f0", "f1"},
                                        new TypeInformation[] {Types.INT, Types.SQL_TIMESTAMP}));

        LOG.info("Submitting job");
        TableResult result = tEnv.executeSql(sqlStatement);

        // Wait for the job to finish.
        JobClient jobClient =
                result.getJobClient()
                        .orElseThrow(() -> new IllegalStateException("Job client is not present"));
        jobClient.getJobExecutionResult().get();

        final String expected =
                "1980,1970-01-01 00:00:00.0\n"
                        + "1980,1970-01-01 00:00:20.0\n"
                        + "1980,1970-01-01 00:00:40.0\n";

        LOG.info("Job finished");
        String actual = new String(Files.readAllBytes(resultFile));
        LOG.info("Actual result is: '{}'", actual);

        assertEquals(expected, actual);
    }

    /** TableSource for generated data. */
    public static class GeneratorTableSource extends InputFormatTableSource<Row> {

        private final int numKeys;
        private final float recordsPerKeyAndSecond;
        private final int durationSeconds;
        private final int offsetSeconds;

        GeneratorTableSource(
                int numKeys, float recordsPerKeyAndSecond, int durationSeconds, int offsetSeconds) {
            this.numKeys = numKeys;
            this.recordsPerKeyAndSecond = recordsPerKeyAndSecond;
            this.durationSeconds = durationSeconds;
            this.offsetSeconds = offsetSeconds;
        }

        @Override
        public InputFormat<Row, ?> getInputFormat() {
            return new IteratorInputFormat<>(
                    DataGenerator.create(
                            numKeys, recordsPerKeyAndSecond, durationSeconds, offsetSeconds));
        }

        @Override
        public DataType getProducedDataType() {
            return getTableSchema().toRowDataType();
        }

        @Override
        public TableSchema getTableSchema() {
            return TableSchema.builder()
                    .field("key", DataTypes.INT())
                    .field("rowtime", DataTypes.TIMESTAMP(3))
                    .field("payload", DataTypes.STRING())
                    .build();
        }
    }

    /** Iterator for generated data. */
    public static class DataGenerator implements Iterator<Row>, Serializable {
        private static final long serialVersionUID = 1L;

        final int numKeys;

        private int keyIndex = 0;

        private final long durationMs;
        private final long stepMs;
        private final long offsetMs;
        private long ms = 0;

        static DataGenerator create(
                int numKeys, float rowsPerKeyAndSecond, int durationSeconds, int offsetSeconds) {
            int sleepMs = (int) (1000 / rowsPerKeyAndSecond);
            return new DataGenerator(
                    numKeys, durationSeconds * 1000, sleepMs, offsetSeconds * 2000L);
        }

        DataGenerator(int numKeys, long durationMs, long stepMs, long offsetMs) {
            this.numKeys = numKeys;
            this.durationMs = durationMs;
            this.stepMs = stepMs;
            this.offsetMs = offsetMs;
        }

        @Override
        public boolean hasNext() {
            return ms < durationMs;
        }

        @Override
        public Row next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Row row =
                    Row.of(
                            keyIndex,
                            LocalDateTime.ofInstant(
                                    Instant.ofEpochMilli(ms + offsetMs), ZoneOffset.UTC),
                            "Some payload...");
            ++keyIndex;
            if (keyIndex >= numKeys) {
                keyIndex = 0;
                ms += stepMs;
            }
            return row;
        }
    }
}
