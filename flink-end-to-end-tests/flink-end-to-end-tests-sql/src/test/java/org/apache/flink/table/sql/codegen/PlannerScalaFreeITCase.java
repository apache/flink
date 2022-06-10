/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.sql.codegen;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.cache.DownloadCache;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.tests.util.flink.SQLJobSubmission;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * End to End tests for table planner scala-free since 1.15. Due to scala-free of table planner
 * introduced, the class in table planner is not visible in distribution runtime, if we use these
 * class in execution time, ClassNotFound exception will be thrown. ITCase in table planner can not
 * cover it, so we should add E2E test for these case.
 */
@RunWith(Parameterized.class)
public class PlannerScalaFreeITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(PlannerScalaFreeITCase.class);

    private static final String SCALA_FREE_E2E_SQL = "scala_free_e2e.sql";

    @Parameterized.Parameters(name = "executionMode")
    public static Collection<String> data() {
        return Arrays.asList("streaming", "batch");
    }

    private static Configuration getConfiguration() {
        // we have to enable checkpoint to trigger flushing for filesystem sink
        final Configuration flinkConfig = new Configuration();
        flinkConfig.setString("execution.checkpointing.interval", "5s");
        return flinkConfig;
    }

    @Rule
    public final FlinkResource flink =
            new LocalStandaloneFlinkResourceFactory()
                    .create(
                            FlinkResourceSetup.builder()
                                    .addConfiguration(getConfiguration())
                                    .build());

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private final String executionMode;
    private Path result;

    @ClassRule public static final DownloadCache DOWNLOAD_CACHE = DownloadCache.get();

    private static final Path sqlToolBoxJar = TestUtils.getResource(".*SqlToolbox.jar");

    public PlannerScalaFreeITCase(String executionMode) {
        this.executionMode = executionMode;
    }

    @Before
    public void before() {
        Path tmpPath = tmp.getRoot().toPath();
        LOG.info("The current temporary path: {}", tmpPath);
        this.result = tmpPath.resolve("result");
    }

    @Test
    public void testImperativeUdaf() throws Exception {
        try (ClusterController clusterController = flink.startCluster(1)) {
            // Initialize the SQL statements from "scala_free_e2e.sql" file
            Map<String, String> varsMap = new HashMap<>();
            varsMap.put("$RESULT", this.result.toAbsolutePath().toString());
            varsMap.put("$MODE", this.executionMode);

            List<String> sqlLines = initializeSqlLines(varsMap);

            // Execute SQL statements in "scala_free_e2e.sql" file
            executeSqlStatements(clusterController, sqlLines);

            // Wait until all the results flushed to the json file.
            LOG.info("Verify the json result.");
            checkJsonResultFile();
            LOG.info("The codegen SQL client test run successfully.");
        }
    }

    private void executeSqlStatements(ClusterController clusterController, List<String> sqlLines)
            throws IOException {
        LOG.info("Executing end-to-end SQL statements {}.", sqlLines);
        clusterController.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJar(sqlToolBoxJar)
                        .build(),
                Duration.ofMinutes(2L));
    }

    private List<String> initializeSqlLines(Map<String, String> vars) throws IOException {
        URL url = PlannerScalaFreeITCase.class.getClassLoader().getResource(SCALA_FREE_E2E_SQL);
        if (url == null) {
            throw new FileNotFoundException(SCALA_FREE_E2E_SQL);
        }

        List<String> lines = Files.readAllLines(new File(url.getFile()).toPath());
        List<String> result = new ArrayList<>();
        for (String line : lines) {
            for (Map.Entry<String, String> var : vars.entrySet()) {
                line = line.replace(var.getKey(), var.getValue());
            }
            result.add(line);
        }

        return result;
    }

    private void checkJsonResultFile() throws Exception {
        boolean success = false;
        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(20));
        List<String> lines = null;
        while (deadline.hasTimeLeft()) {
            if (Files.exists(result)) {
                lines = readJsonResultFiles(result);
                if (lines.size() == 2) {
                    success = true;
                    assertThat(
                            lines.toArray(new String[0]),
                            arrayContainingInAnyOrder(
                                    "{\"before\":null,\"after\":{\"user_name\":\"Alice\",\"order_cnt\":1},\"op\":\"c\"}",
                                    "{\"before\":null,\"after\":{\"user_name\":\"Bob\",\"order_cnt\":2},\"op\":\"c\"}"));
                    break;
                } else {
                    LOG.info(
                            "The target Json {} does not contain enough records, current {} records, left time: {}s",
                            result,
                            lines.size(),
                            deadline.timeLeft().getSeconds());
                }
            } else {
                LOG.info("The target Json {} does not exist now", result);
            }
            Thread.sleep(500);
        }
        assertTrue(
                String.format(
                        "Did not get expected results before timeout, actual result: %s.", lines),
                success);
    }

    private static List<String> readJsonResultFiles(Path path) throws IOException {
        File filePath = path.toFile();
        // list all the non-hidden files
        File[] csvFiles = filePath.listFiles((dir, name) -> !name.startsWith("."));
        List<String> result = new ArrayList<>();
        if (csvFiles != null) {
            for (File file : csvFiles) {
                result.addAll(Files.readAllLines(file.toPath()));
            }
        }
        return result;
    }
}
