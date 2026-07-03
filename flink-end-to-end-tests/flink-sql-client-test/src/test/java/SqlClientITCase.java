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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.connector.upserttest.sink.UpsertTestFileUtil;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** E2E Test for SqlClient. */
class SqlClientITCase {

    private static final Logger LOG = LoggerFactory.getLogger(SqlClientITCase.class);

    private final Path sqlConnectorUpsertTestJar =
            ResourceTestUtils.getResource(".*flink-test-utils.*\\.jar");

    private final FlinkContainers flink =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(
                            FlinkContainersSettings.builder()
                                    .numTaskManagers(1)
                                    // enable checkpointing for the UpsertTestSink to write anything
                                    .setConfigOption(
                                            CheckpointingOptions.CHECKPOINTING_INTERVAL,
                                            Duration.ofMillis(500))
                                    .build())
                    .withTestcontainersSettings(
                            TestcontainersSettings.builder().logger(LOG).build())
                    .build();

    @TempDir private File tempDir;

    @BeforeEach
    void setup() throws Exception {
        flink.start();
    }

    @AfterEach
    void tearDown() {
        flink.stop();
    }

    @Test
    void testUpsert() throws Exception {
        String outputFilepath = "/flink/records-upsert.out";

        List<String> sqlLines =
                List.of(
                        "SET 'execution.runtime-mode' = 'batch';",
                        "",
                        "CREATE TABLE UpsertSinkTable (",
                        "    user_id INT,",
                        "    user_name STRING,",
                        "    user_count BIGINT,",
                        "    PRIMARY KEY (user_id) NOT ENFORCED",
                        "  ) WITH (",
                        "    'connector' = 'upsert-files',",
                        "    'key.format' = 'json',",
                        "    'value.format' = 'json',",
                        "    'output-filepath' = '" + outputFilepath + "'",
                        "  );",
                        "",
                        "INSERT INTO UpsertSinkTable(",
                        "  SELECT user_id, user_name, COUNT(*) AS user_count",
                        "  FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), (42, 'Kim'), (42, 'Kim'), (1, 'Bob'))",
                        "    AS UserCountTable(user_id, user_name)",
                        "  GROUP BY user_id, user_name);");
        executeSql(sqlLines);

        /*
        | user_id | user_name | user_count |
        | ------- | --------- | ---------- |
        |    1    |    Bob    |     2      |
        |   22    |    Tom    |     1      |
        |   42    |    Kim    |     3      |
        */

        verifyNumberOfResultRecords(outputFilepath, 3);
    }

    @Test
    void testAppendNoKey() throws Exception {
        String outputFilepath = "/flink/records-append.out";

        List<String> sqlLines =
                List.of(
                        "SET 'execution.runtime-mode' = 'batch';",
                        "",
                        "CREATE TABLE AppendSinkTable (",
                        "    user_id INT,",
                        "    user_name STRING,",
                        "    user_count BIGINT",
                        "  ) WITH (",
                        "    'connector' = 'upsert-files',",
                        "    'key.format' = 'json',",
                        "    'value.format' = 'json',",
                        "    'output-filepath' = '" + outputFilepath + "'",
                        "  );",
                        "",
                        "INSERT INTO AppendSinkTable",
                        "  SELECT *",
                        "  FROM (",
                        "    VALUES",
                        "      (1, 'Bob', CAST(0 AS BIGINT)),",
                        "      (22, 'Tom', CAST(0 AS BIGINT)),",
                        "      (42, 'Kim', CAST(0 AS BIGINT)),",
                        "      (42, 'Kim', CAST(0 AS BIGINT)),",
                        "      (42, 'Kim', CAST(0 AS BIGINT)),",
                        "      (1, 'Bob', CAST(0 AS BIGINT)))",
                        "    AS UserCountTable(user_id, user_name, user_count);");
        executeSql(sqlLines);

        verifyNumberOfResultRecords(outputFilepath, 3);
    }

    @Test
    void testMatchRecognize() throws Exception {
        String outputFilepath = "/flink/records-matchrecognize.out";
        String inputFilepath = "/tmp/test-json.jsonl";

        List<String> messages =
                List.of(
                        "{\"timestamp\": \"2018-03-12T08:00:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
                        "{\"timestamp\": \"2018-03-12T08:10:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
                        "{\"timestamp\": \"2018-03-12T09:00:00Z\", \"user\": \"Bob\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is another warning.\"}}",
                        "{\"timestamp\": \"2018-03-12T09:10:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"INFO\", \"message\": \"This is a info.\"}}",
                        "{\"timestamp\": \"2018-03-12T09:20:00Z\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
                        "{\"timestamp\": \"2018-03-12T09:30:00Z\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
                        "{\"timestamp\": \"2018-03-12T09:30:00Z\", \"user\": null, \"event\": { \"type\": \"WARNING\", \"message\": \"This is a bad message because the user is missing.\"}}",
                        "{\"timestamp\": \"2018-03-12T10:40:00Z\", \"user\": \"Bob\", \"event\": { \"type\": \"ERROR\", \"message\": \"This is an error.\"}}");
        File inputFile = new File(tempDir, "test-json.jsonl");
        Files.write(inputFile.toPath(), messages);
        MountableFile mountableFile = MountableFile.forHostPath(inputFile.toPath());
        flink.getJobManager().copyFileToContainer(mountableFile, inputFilepath);
        for (GenericContainer<?> taskManager : flink.getTaskManagers()) {
            taskManager.copyFileToContainer(mountableFile, inputFilepath);
        }

        List<String> sqlLines =
                List.of(
                        // MATCH_RECOGNIZE requires streaming mode; the bounded filesystem source
                        // emits MAX_WATERMARK at end of input which flushes the pending match
                        "SET 'execution.runtime-mode' = 'streaming';",
                        "",
                        "CREATE TABLE JsonSourceTable (",
                        "    `timestamp` TIMESTAMP_LTZ(3),",
                        "    `user` STRING,",
                        "    `event` ROW<type STRING, message STRING>,",
                        "    `rowtime` AS `timestamp`,",
                        "    WATERMARK FOR `rowtime` AS `rowtime` - INTERVAL '2' SECOND",
                        ") WITH (",
                        "    'connector' = 'filesystem',",
                        "    'path' = 'file://" + inputFilepath + "',",
                        "    'format' = 'json',",
                        "    'json.timestamp-format.standard' = 'ISO-8601'",
                        ");",
                        "",
                        "CREATE TABLE AppendSinkTable (",
                        "    user_id INT,",
                        "    user_name STRING,",
                        "    user_count BIGINT",
                        "  ) WITH (",
                        "    'connector' = 'upsert-files',",
                        "    'key.format' = 'json',",
                        "    'value.format' = 'json',",
                        "    'output-filepath' = '" + outputFilepath + "'",
                        "  );",
                        "",
                        "INSERT INTO AppendSinkTable",
                        "  SELECT 1 as user_id, T.userName as user_name, cast(1 as BIGINT) as user_count",
                        "  FROM (",
                        "    SELECT `user`, `rowtime`",
                        "    FROM JsonSourceTable",
                        "    WHERE `user` IS NOT NULL)",
                        "  MATCH_RECOGNIZE (",
                        "    ORDER BY rowtime",
                        "    MEASURES",
                        "        `user` as userName",
                        "    PATTERN (A)",
                        "    DEFINE",
                        "        A as `user` = 'Alice'",
                        "  ) as T;");
        executeSql(sqlLines);

        verifyNumberOfResultRecords(outputFilepath, 1);
    }

    private void verifyNumberOfResultRecords(String resultFilePath, int expectedNumberOfRecords)
            throws Exception {
        RestClusterClient<StandaloneClusterId> clusterClient = flink.getRestClusterClient();
        // the sink flushes all records at the latest when the bounded job finishes, so the
        // result file is complete once the job reaches a terminal state
        CommonTestUtils.waitUntilIgnoringExceptions(
                () -> {
                    try {
                        Collection<JobStatusMessage> jobs = clusterClient.listJobs().get();
                        return !jobs.isEmpty()
                                && jobs.stream()
                                        .allMatch(
                                                job -> job.getJobState().isGloballyTerminalState());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                Duration.ofMinutes(2),
                Duration.ofMillis(100),
                "Job did not terminate within 2 minutes");
        Collection<JobStatusMessage> jobs = clusterClient.listJobs().get();
        assertThat(jobs).hasSize(1);
        assertThat(jobs.iterator().next().getJobState()).isEqualTo(JobStatus.FINISHED);

        File tempOutputFile = new File(tempDir, "records.out");
        flink.getTaskManagers()
                .get(0)
                .copyFileFromContainer(resultFilePath, tempOutputFile.toString());
        assertThat(UpsertTestFileUtil.getNumberOfRecords(tempOutputFile))
                .isEqualTo(expectedNumberOfRecords);
    }

    private void executeSql(List<String> sqlLines) throws Exception {
        flink.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJars(sqlConnectorUpsertTestJar)
                        .build());
    }
}
