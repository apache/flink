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

package org.apache.flink.connector.upserttest.table;

import org.apache.flink.connector.upserttest.sink.UpsertTestFileUtil;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Tests for {@link UpsertTestDynamicTableSink}. */
@ExtendWith(TestLoggerExtension.class)
class UpsertTestDynamicTableSinkITCase {

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @Test
    void testWritingDocumentsInBatchMode(@TempDir File tempDir) throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        String format = "json";
        File outputFile = new File(tempDir, "records.out");
        String outputFilepath = outputFile.toString();

        final String createTable =
                String.format(
                        "CREATE TABLE UpsertFileSinkTable (\n"
                                + "    user_id INT,\n"
                                + "    user_name STRING,\n"
                                + "    user_count BIGINT,\n"
                                + "    PRIMARY KEY (user_id) NOT ENFORCED\n"
                                + "  ) WITH (\n"
                                + "    'connector' = '%s',\n"
                                + "    'key.format' = '%s',\n"
                                + "    'value.format' = '%s',\n"
                                + "    'output-filepath' = '%s'\n"
                                + "  );",
                        UpsertTestDynamicTableSinkFactory.IDENTIFIER,
                        format,
                        format,
                        outputFilepath);

        tEnv.executeSql(createTable);

        String insertSql =
                "INSERT INTO UpsertFileSinkTable\n"
                        + "SELECT user_id, user_name, COUNT(*) AS user_count\n"
                        + "FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), (42, 'Kim'), (42, 'Kim'), (1, 'Bob'))\n"
                        + "  AS UserCountTable(user_id, user_name)\n"
                        + "GROUP BY user_id, user_name";
        tEnv.executeSql(insertSql).await();

        /*
        | user_id | user_name | user_count |
        | ------- | --------- | ---------- |
        |    1    |    Bob    |     2      |
        |   22    |    Tom    |     1      |
        |   42    |    Kim    |     3      |
        */

        int numberOfResultRecords = UpsertTestFileUtil.getNumberOfRecords(outputFile);
        assertThat(numberOfResultRecords).isEqualTo(3);
    }
}
