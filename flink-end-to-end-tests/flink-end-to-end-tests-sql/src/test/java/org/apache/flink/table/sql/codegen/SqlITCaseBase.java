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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Rule;
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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Base class for sql ITCase. */
@RunWith(Parameterized.class)
public abstract class SqlITCaseBase extends TestLogger {
    private static final Logger LOG = LoggerFactory.getLogger(SqlITCaseBase.class);

    @Parameterized.Parameters(name = "executionMode")
    public static Collection<String> data() {
        return Arrays.asList("streaming", "batch");
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

    protected static final Path SQL_TOOL_BOX_JAR = TestUtils.getResource(".*SqlToolbox.jar");

    public SqlITCaseBase(String executionMode) {
        this.executionMode = executionMode;
    }

    private static Configuration getConfiguration() {
        // we have to enable checkpoint to trigger flushing for filesystem sink
        final Configuration flinkConfig = new Configuration();
        flinkConfig.setString("execution.checkpointing.interval", "5s");
        return flinkConfig;
    }

    @Before
    public void before() throws Exception {
        Path tmpPath = tmp.getRoot().toPath();
        LOG.info("The current temporary path: {}", tmpPath);
        this.result = tmpPath.resolve(String.format("result-%s", UUID.randomUUID()));
    }

    public void runAndCheckSQL(
            String sqlPath, Map<String, String> varsMap, int resultSize, List<String> resultItems)
            throws Exception {
        try (ClusterController clusterController = flink.startCluster(1)) {
            List<String> sqlLines = initializeSqlLines(sqlPath, varsMap);

            executeSqlStatements(clusterController, sqlLines);

            // Wait until all the results flushed to the json file.
            LOG.info("Verify the json result.");
            checkJsonResultFile(resultSize, resultItems);
            LOG.info("The SQL client test run successfully.");
        }
    }

    protected Map<String, String> generateReplaceVars() {
        Map<String, String> varsMap = new HashMap<>();
        varsMap.put("$RESULT", this.result.toAbsolutePath().toString());
        varsMap.put("$MODE", this.executionMode);
        return varsMap;
    }

    protected abstract void executeSqlStatements(
            ClusterController clusterController, List<String> sqlLines) throws Exception;

    private List<String> initializeSqlLines(String sqlPath, Map<String, String> vars)
            throws IOException {
        URL url = SqlITCaseBase.class.getClassLoader().getResource(sqlPath);
        if (url == null) {
            throw new FileNotFoundException(sqlPath);
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

    private void checkJsonResultFile(int resultSize, List<String> items) throws Exception {
        boolean success = false;
        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(20));
        List<String> lines = null;
        while (deadline.hasTimeLeft()) {
            if (Files.exists(result)) {
                lines = readJsonResultFiles(result);
                if (lines.size() == resultSize) {
                    success = true;
                    assertThat(lines).hasSameElementsAs(items);
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
                success,
                String.format(
                        "Did not get expected results before timeout, actual result: %s.", lines));
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
