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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.io.IteratorInputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * End-to-end test for batch SQL queries.
 *
 * <p>The sources are generated and bounded. The result is always constant.
 *
 * <p>Parameters: -outputPath output file path for CsvTableSink; -sqlStatement SQL statement that
 * will be executed as executeSql
 */
public class BatchSQLTestProgram {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String outputPath = params.getRequired("outputPath");
        String sqlStatement = params.getRequired("sqlStatement");

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        ((TableEnvironmentInternal) tEnv)
                .registerTableSourceInternal("table1", new GeneratorTableSource(10, 100, 60, 0));
        ((TableEnvironmentInternal) tEnv)
                .registerTableSourceInternal("table2", new GeneratorTableSource(5, 0.2f, 60, 5));
        tEnv.createTemporaryTable(
                "sinkTable",
                TableDescriptor.forConnector("filesystem")
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT())
                                        .column("f1", DataTypes.TIMESTAMP(3))
                                        .build())
                        .option(FileSystemConnectorOptions.PATH, outputPath)
                        .format("csv")
                        .build());

        TableResult result = tEnv.executeSql(sqlStatement);
        // wait job finish
        result.getJobClient().get().getJobExecutionResult().get();
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
