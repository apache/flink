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

package org.apache.flink.tests.util.hive;

import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.cache.DownloadCache;
import org.apache.flink.tests.util.categories.PreCommit;
import org.apache.flink.tests.util.categories.TravisGroup1;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.tests.util.flink.SQLJobSubmission;
import org.apache.flink.tests.util.hvie.HiveResource;
import org.apache.flink.testutils.junit.FailsOnJava11;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** End-to-end test for the HBase connectors. */
@RunWith(Parameterized.class)
@Category(value = {TravisGroup1.class, PreCommit.class, FailsOnJava11.class})
@Ignore("FLINK-21519")
public class SQLClientHiveITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(SQLClientHiveITCase.class);

    private static final String HBASE_E2E_SQL = "hive_e2e.sql";

    @Parameterized.Parameters(name = "{index}: hbase-version:{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {"1.4.3", "hbase-1.4"},
                    {"2.2.3", "hbase-2.2"}
                });
    }

    @Rule public final HiveResource hive;

    @Rule
    public final FlinkResource flink =
            new LocalStandaloneFlinkResourceFactory().create(FlinkResourceSetup.builder().build());

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private final String hiveConnector;
    private final Path sqlConnectorHBaseJar;

    @ClassRule public static final DownloadCache DOWNLOAD_CACHE = DownloadCache.get();

    private static final Path sqlToolBoxJar = TestUtils.getResource(".*SqlToolbox.jar");
    private static final Path hadoopClasspath = TestUtils.getResource(".*hadoop.classpath");
    private List<Path> hadoopClasspathJars;

    public SQLClientHiveITCase(String hiveVersion, String hiveConnector) {
        this.hive = HiveResource.get(hiveVersion);
        this.hiveConnector = hiveConnector;
        this.sqlConnectorHBaseJar = TestUtils.getResource(".*sql-" + hiveConnector + ".jar");
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

    private List<String> initializeSqlLines(Map<String, String> vars) throws IOException {
        URL url = SQLClientHiveITCase.class.getClassLoader().getResource(HBASE_E2E_SQL);
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
