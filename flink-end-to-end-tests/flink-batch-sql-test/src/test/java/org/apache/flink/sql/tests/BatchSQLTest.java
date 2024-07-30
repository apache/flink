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
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.formats.csv.CsvFormatOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            names = {"ALL_EXCHANGES_BLOCKING"
                // Remove hybrid shuffle types to unblock CI. This have to reactive when we find the
                // reason.
                // "ALL_EXCHANGES_HYBRID_FULL",
                // "ALL_EXCHANGES_HYBRID_SELECTIVE"
                // Only above shuffle modes are supported by the adaptive batch scheduler
                // , "ALL_EXCHANGES_PIPELINE"
            })
    public void testBatchSQL(BatchShuffleMode shuffleMode, @TempDir Path tmpDir) throws Exception {
        LOG.info("Results for this test will be stored at: {}", tmpDir);

        String sqlStatement = new String(Files.readAllBytes(sqlPath));

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set(ExecutionOptions.BATCH_SHUFFLE_MODE, shuffleMode);

        tEnv.createTable(
                "table1",
                TableDescriptor.forConnector(GeneratorTableSourceFactory.CONNECTOR_ID)
                        .schema(GeneratorTableSourceFactory.getSchema())
                        .option(GeneratorTableSourceFactory.NUM_KEYS, 10)
                        .option(GeneratorTableSourceFactory.ROWS_PER_KEY_PER_SECOND, 100f)
                        .option(GeneratorTableSourceFactory.DURATION_SECONDS, 60)
                        .option(GeneratorTableSourceFactory.OFFSET_SECONDS, 0)
                        .build());
        tEnv.createTable(
                "table2",
                TableDescriptor.forConnector(GeneratorTableSourceFactory.CONNECTOR_ID)
                        .schema(GeneratorTableSourceFactory.getSchema())
                        .option(GeneratorTableSourceFactory.NUM_KEYS, 5)
                        .option(GeneratorTableSourceFactory.ROWS_PER_KEY_PER_SECOND, 0.2f)
                        .option(GeneratorTableSourceFactory.DURATION_SECONDS, 60)
                        .option(GeneratorTableSourceFactory.OFFSET_SECONDS, 5)
                        .build());

        tEnv.createTable(
                "sinkTable",
                TableDescriptor.forConnector("filesystem")
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT())
                                        .column("f1", DataTypes.TIMESTAMP(3))
                                        .build())
                        .option("path", tmpDir.toString())
                        .format(
                                FormatDescriptor.forFormat("csv")
                                        .option(CsvFormatOptions.FIELD_DELIMITER, ",")
                                        .option(CsvFormatOptions.DISABLE_QUOTE_CHARACTER, true)
                                        .build())
                        .build());

        LOG.info("Submitting job");
        TableResult result = tEnv.executeSql(sqlStatement);

        // Wait for the job to finish.
        JobClient jobClient =
                result.getJobClient()
                        .orElseThrow(() -> new IllegalStateException("Job client is not present"));
        jobClient.getJobExecutionResult().get();

        final String expected =
                "1980,1970-01-01 00:00:00\n"
                        + "1980,1970-01-01 00:00:20\n"
                        + "1980,1970-01-01 00:00:40\n";

        LOG.info("Job finished");

        List<String> files =
                Stream.of(Objects.requireNonNull(new File(tmpDir.toString()).listFiles()))
                        .filter(file -> !file.isDirectory())
                        .map(File::getPath)
                        .collect(Collectors.toList());
        assertEquals(1, files.size());
        Path resultFile = Paths.get(files.get(0));

        LOG.info("Result found at {}", resultFile);
        String actual = new String(Files.readAllBytes(resultFile));
        LOG.info("Actual result is: '{}'", actual);

        assertEquals(expected, actual);
    }
}
