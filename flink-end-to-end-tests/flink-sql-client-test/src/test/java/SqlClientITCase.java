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

import org.apache.flink.connector.upserttest.sink.UpsertTestFileUtil;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.SQLJobSubmission;
import org.apache.flink.tests.util.flink.container.FlinkContainers;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** E2E Test for SqlClient. */
public class SqlClientITCase {

    private static final Logger LOG = LoggerFactory.getLogger(SqlClientITCase.class);

    private static final Path sqlToolBoxJar = TestUtils.getResource(".*SqlToolbox.jar");

    private final Path sqlConnectorUpsertTestJar =
            TestUtils.getResource(".*flink-test-utils.*.jar");

    public static final Network NETWORK = Network.newNetwork();

    public final FlinkContainers flink =
            FlinkContainers.builder()
                    .setNumTaskManagers(1)
                    .setNetwork(NETWORK)
                    .setLogger(LOG) /*.dependsOn(KAFKA)*/
                    .build();

    @TempDir private File tempDir;

    @BeforeEach
    public void setup() throws Exception {
        flink.start();
    }

    @AfterEach
    public void tearDown() {
        flink.stop();
    }

    @Test
    public void testUpsert() throws Exception {
        String outputFilepath = "/flink/records.out";

        List<String> sqlLines =
                Arrays.asList(
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

        Thread.sleep(5000); // prevent NotFoundException: Status 404

        verifyNumberOfResultRecords(outputFilepath, 3);
    }

    private void verifyNumberOfResultRecords(String resultFilePath, int expectedNumberOfRecords)
            throws IOException {
        File tempOutputFile = new File(tempDir, "records.out");
        String tempOutputFilepath = tempOutputFile.toString();
        GenericContainer<?> taskManager = flink.getTaskManagers().get(0);
        taskManager.copyFileFromContainer(resultFilePath, tempOutputFilepath);

        int numberOfResultRecords = UpsertTestFileUtil.getNumberOfRecords(tempOutputFile);
        assertThat(numberOfResultRecords).isEqualTo(expectedNumberOfRecords);
    }

    private void executeSql(List<String> sqlLines) throws Exception {
        flink.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJars(sqlConnectorUpsertTestJar, sqlToolBoxJar)
                        .build());
    }
}
