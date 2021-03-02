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

package org.apache.flink.tests.util.hbase;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.cache.DownloadCache;
import org.apache.flink.tests.util.categories.PreCommit;
import org.apache.flink.tests.util.categories.TravisGroup1;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.tests.util.flink.SQLJobSubmission;
import org.apache.flink.testutils.junit.FailsOnJava11;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

/** End-to-end test for the HBase connectors. */
@Ignore // disabled because of stalling
@RunWith(Parameterized.class)
@Category(value = {TravisGroup1.class, PreCommit.class, FailsOnJava11.class})
public class SQLClientHBaseITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(SQLClientHBaseITCase.class);

    private static final String HBASE_E2E_SQL = "hbase_e2e.sql";

    @Parameterized.Parameters(name = "{index}: hbase-version:{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {"1.4.3", "hbase-1.4"},
                    {"2.2.3", "hbase-2.2"}
                });
    }

    @Rule public final HBaseResource hbase;

    @Rule
    public final FlinkResource flink =
            new LocalStandaloneFlinkResourceFactory().create(FlinkResourceSetup.builder().build());

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private final String hbaseConnector;
    private final Path sqlConnectorHBaseJar;

    @ClassRule public static final DownloadCache DOWNLOAD_CACHE = DownloadCache.get();

    private static final Path sqlToolBoxJar = TestUtils.getResource(".*SqlToolbox.jar");
    private static final Path hadoopClasspath = TestUtils.getResource(".*hadoop.classpath");
    private List<Path> hadoopClasspathJars;

    public SQLClientHBaseITCase(String hbaseVersion, String hbaseConnector) {
        this.hbase = HBaseResource.get(hbaseVersion);
        this.hbaseConnector = hbaseConnector;
        this.sqlConnectorHBaseJar = TestUtils.getResource(".*sql-" + hbaseConnector + ".jar");
    }

    @Before
    public void before() throws Exception {
        DOWNLOAD_CACHE.before();
        Path tmpPath = tmp.getRoot().toPath();
        LOG.info("The current temporary path: {}", tmpPath);

        // Prepare all hadoop jars to mock HADOOP_CLASSPATH, use hadoop.classpath which contains all
        // hadoop jars
        File hadoopClasspathFile = new File(hadoopClasspath.toAbsolutePath().toString());

        if (!hadoopClasspathFile.exists()) {
            throw new FileNotFoundException(
                    "File that contains hadoop classpath "
                            + hadoopClasspath.toString()
                            + " does not exist.");
        }

        String classPathContent = FileUtils.readFileUtf8(hadoopClasspathFile);
        hadoopClasspathJars =
                Arrays.stream(classPathContent.split(":"))
                        .map(jar -> Paths.get(jar))
                        .collect(Collectors.toList());
    }

    @Test
    public void testHBase() throws Exception {
        try (ClusterController clusterController = flink.startCluster(2)) {
            // Create table and put data
            hbase.createTable("source", "family1", "family2");
            hbase.createTable("sink", "family1", "family2");
            hbase.putData("source", "row1", "family1", "f1c1", "v1");
            hbase.putData("source", "row1", "family2", "f2c1", "v2");
            hbase.putData("source", "row1", "family2", "f2c2", "v3");
            hbase.putData("source", "row2", "family1", "f1c1", "v4");
            hbase.putData("source", "row2", "family2", "f2c1", "v5");
            hbase.putData("source", "row2", "family2", "f2c2", "v6");

            // Initialize the SQL statements from "hbase_e2e.sql" file
            Map<String, String> varsMap = new HashMap<>();
            varsMap.put("$HBASE_CONNECTOR", hbaseConnector);
            List<String> sqlLines = initializeSqlLines(varsMap);

            // Execute SQL statements in "hbase_e2e.sql" file
            executeSqlStatements(clusterController, sqlLines);

            LOG.info("Verify the sink table result.");
            // Wait until all the results flushed to the HBase sink table.
            checkHBaseSinkResult();
            LOG.info("The HBase SQL client test run successfully.");
        }
    }

    private void checkHBaseSinkResult() throws Exception {
        boolean success = false;
        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(120));
        while (deadline.hasTimeLeft()) {
            final List<String> lines = hbase.scanTable("sink");
            if (lines.size() == 6) {
                success = true;
                assertThat(
                        lines.toArray(new String[0]),
                        arrayContainingInAnyOrder(
                                CoreMatchers.allOf(
                                        containsString("row1"),
                                        containsString("family1"),
                                        containsString("f1c1"),
                                        containsString("value1")),
                                CoreMatchers.allOf(
                                        containsString("row1"),
                                        containsString("family2"),
                                        containsString("f2c1"),
                                        containsString("v2")),
                                CoreMatchers.allOf(
                                        containsString("row1"),
                                        containsString("family2"),
                                        containsString("f2c2"),
                                        containsString("v3")),
                                CoreMatchers.allOf(
                                        containsString("row2"),
                                        containsString("family1"),
                                        containsString("f1c1"),
                                        containsString("value4")),
                                CoreMatchers.allOf(
                                        containsString("row2"),
                                        containsString("family2"),
                                        containsString("f2c1"),
                                        containsString("v5")),
                                CoreMatchers.allOf(
                                        containsString("row2"),
                                        containsString("family2"),
                                        containsString("f2c2"),
                                        containsString("v6"))));
                break;
            } else {
                LOG.info(
                        "The HBase sink table does not contain enough records, current {} records, left time: {}s",
                        lines.size(),
                        deadline.timeLeft().getSeconds());
            }
            Thread.sleep(500);
        }
        Assert.assertTrue("Did not get expected results before timeout.", success);
    }

    private List<String> initializeSqlLines(Map<String, String> vars) throws IOException {
        URL url = SQLClientHBaseITCase.class.getClassLoader().getResource(HBASE_E2E_SQL);
        if (url == null) {
            throw new FileNotFoundException(HBASE_E2E_SQL);
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

    private void executeSqlStatements(ClusterController clusterController, List<String> sqlLines)
            throws IOException {
        LOG.info("Executing SQL: HBase source table -> HBase sink table");
        clusterController.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJar(sqlToolBoxJar)
                        .addJar(sqlConnectorHBaseJar)
                        .addJars(hadoopClasspathJars)
                        .build(),
                Duration.ofMinutes(2L));
    }
}
