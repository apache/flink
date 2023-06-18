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

package org.apache.flink.table.sql.codegen;

import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.tests.util.flink.ClusterController;

import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * End-to-End tests for table planner scala-free since 1.15. Due to scala-free of table planner
 * introduced, the class in table planner is not visible in distribution runtime, if we use these
 * class in execution time, ClassNotFound exception will be thrown. ITCase in table planner can not
 * cover it, so we should add E2E test for these case.
 */
public class PlannerScalaFreeITCase extends SqlITCaseBase {
    public PlannerScalaFreeITCase(String executionMode) {
        super(executionMode);
    }

    @Test
    public void testImperativeUdaf() throws Exception {
        runAndCheckSQL(
                "scala_free_e2e.sql",
                Arrays.asList(
                        "{\"before\":null,\"after\":{\"user_name\":\"Alice\",\"order_cnt\":1},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"user_name\":\"Bob\",\"order_cnt\":2},\"op\":\"c\"}"));
    }

    @Override
    protected void executeSqlStatements(ClusterController clusterController, List<String> sqlLines)
            throws Exception {
        clusterController.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJar(SQL_TOOL_BOX_JAR)
                        .build(),
                Duration.ofMinutes(2L));
    }
}
