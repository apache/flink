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
import org.apache.flink.test.resources.ResourceTestUtils;
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
import java.util.Collections;
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

    protected final String executionMode;

    protected Path result;

    protected static final Path SQL_TOOL_BOX_JAR =
            ResourceTestUtils.getResource(".*SqlToolbox.jar");

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

    public void runAndCheckSQL(String sqlPath, List<String> resultItems) throws Exception {
        runAndCheckSQL(sqlPath, Collections.singletonMap(result, resultItems));
    }

    public void runAndCheckSQL(String sqlPath, Map<Path, List<String>> resultItems)
            throws Exception {
        try (ClusterController clusterController = flink.startCluster(1)) {
            List<String> sqlLines = initializeSqlLines(sqlPath);

            executeSqlStatements(clusterController, sqlLines);

            // Wait until all the results flushed to the json file.
            LOG.info("Verify the result.");
            for (Map.Entry<Path, List<String>> entry : resultItems.entrySet()) {
                checkResultFile(entry.getKey(), entry.getValue());
            }
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

    private List<String> initializeSqlLines(String sqlPath) throws IOException {
        URL url = SqlITCaseBase.class.getClassLoader().getResource(sqlPath);
        if (url == null) {
            throw new FileNotFoundException(sqlPath);
        }
        Map<String, String> vars = generateReplaceVars();
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

    private static void checkResultFile(Path resultPath, List<String> expectedItems)
            throws Exception {
        boolean success = false;
        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(20));
        List<String> lines = null;
        int resultSize = expectedItems.size();
        while (deadline.hasTimeLeft()) {
            if (Files.exists(resultPath)) {
                lines = readResultFiles(resultPath);
                if (lines.size() == resultSize) {
                    success = true;
                    assertThat(lines).hasSameElementsAs(expectedItems);
                    break;
                } else {
                    LOG.info(
                            "The target result {} does not contain enough records, current {} records, left time: {}s",
                            resultPath,
                            lines.size(),
                            deadline.timeLeft().getSeconds());
                }
            } else {
                LOG.info("The target result {} does not exist now", resultPath);
            }
            Thread.sleep(500);
        }
        assertTrue(
                success,
                String.format(
                        "Did not get expected results before timeout, actual result: %s.", lines));
    }

    private static List<String> readResultFiles(Path path) throws IOException {
        File filePath = path.toFile();
        // list all the non-hidden files
        File[] files = filePath.listFiles((dir, name) -> !name.startsWith("."));
        List<String> result = new ArrayList<>();
        if (files != null) {
            for (File file : files) {
                result.addAll(Files.readAllLines(file.toPath()));
            }
        }
        return result;
    }
}
