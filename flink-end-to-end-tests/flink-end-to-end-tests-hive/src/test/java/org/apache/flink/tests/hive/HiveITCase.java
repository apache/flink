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

package org.apache.flink.tests.hive;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.tests.hive.containers.HiveContainers;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.JarLocation;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.UserClassLoaderJarTestUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** IT case for Hive including reading & writing hive and Hive dialect. */
public class HiveITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(HiveITCase.class);

    @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    @ClassRule
    public static final HiveContainers.HiveContainer HIVE_CONTAINER =
            HiveContainers.createHiveContainer(
                    Arrays.asList("hive_sink1", "hive_sink2", "h_table_sink1", "h_table_sink2"));

    private static final String HIVE_ADD_ONE_UDF_CLASS = "HiveAddOneFunc";
    private static final String HIVE_ADD_ONE_UDF_CODE =
            "public class "
                    + HIVE_ADD_ONE_UDF_CLASS
                    + " extends org.apache.hadoop.hive.ql.exec.UDF {\n"
                    + " public int evaluate(int content) {\n"
                    + "    return content + 1;\n"
                    + " }"
                    + "}\n";

    private static final Path HADOOP_CLASS_PATH =
            ResourceTestUtils.getResource(".*hadoop.classpath");
    private static final Path sqlHiveJar = ResourceTestUtils.getResource(".*sql-hive-.*.jar");
    private static String hiveConfDirPath;
    private static String udfDependsPath;

    @Rule public final FlinkResource flink = buildFlinkResource();

    @BeforeClass
    public static void beforeClass() throws Exception {
        initUDFJar();
        initHiveConfFile();
    }

    private static void initUDFJar() throws Exception {
        Path tmpPath = TMP_FOLDER.getRoot().toPath();
        LOG.info("The current temporary path: {}", tmpPath);
        Map<String, String> classNameCodes = new HashMap<>();
        classNameCodes.put(HIVE_ADD_ONE_UDF_CLASS, HIVE_ADD_ONE_UDF_CODE);
        File udfJar =
                UserClassLoaderJarTestUtils.createJarFile(
                        TMP_FOLDER.newFolder("test-jar"),
                        "test-classloader-udf.jar",
                        classNameCodes);
        udfDependsPath = udfJar.toURI().getPath();
    }

    private static void initHiveConfFile() {
        // create a hive conf file
        File hiveSiteFile = createHiveConfFile();
        hiveConfDirPath = hiveSiteFile.getParent();
    }

    @Test
    public void testReadWriteHive() throws Exception {
        runAndCheckSQL(
                "hive_catalog_read_write_hive.sql",
                generateReplaceVars(),
                Paths.get(HIVE_CONTAINER.getWarehousePath()).resolve("hive_sink2"),
                2,
                Arrays.asList("1\u0001v1", "2\u0001v2"));
    }

    @Test
    public void testHiveDialect() throws Exception {
        runAndCheckSQL(
                "hive_dialect.sql",
                generateReplaceVars(),
                Paths.get(HIVE_CONTAINER.getWarehousePath()).resolve("h_table_sink2"),
                3,
                Arrays.asList("2\u0001v1", "3\u0001v2", "4\u0001v3"));
    }

    private Map<String, String> generateReplaceVars() {
        Map<String, String> varsMap = new HashMap<>();
        varsMap.put("$VAR_UDF_JAR_PATH", udfDependsPath);
        varsMap.put("$VAR_HIVE_CONF_DIR", hiveConfDirPath);
        varsMap.put("$VAR_HIVE_WAREHOUSE", "file://" + HIVE_CONTAINER.getWarehousePath());
        return varsMap;
    }

    private void runAndCheckSQL(
            String sqlPath,
            Map<String, String> varsMap,
            Path resultPath,
            int resultSize,
            List<String> resultItems)
            throws Exception {
        try (ClusterController clusterController = flink.startCluster(1)) {
            List<String> sqlLines = initializeSqlLines(sqlPath, varsMap);

            executeSqlStatements(clusterController, sqlLines);

            // Wait until all the results flushed to the json file.
            LOG.info("Verify the result file.");
            checkResultFile(resultPath, resultSize, resultItems);
            LOG.info("The SQL client test run successfully.");
        }
    }

    private List<String> readResultFiles(Path path) throws IOException {
        File filePath = path.toFile();
        // list all the non-hidden files
        File[] txtFiles = filePath.listFiles((dir, name) -> !name.startsWith("."));
        List<String> result = new ArrayList<>();
        if (txtFiles != null) {
            for (File file : txtFiles) {
                result.addAll(Files.readAllLines(file.toPath()));
            }
        }
        return result;
    }

    private void checkResultFile(Path resultPath, int resultSize, List<String> items)
            throws Exception {
        boolean success = false;
        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(20));
        List<String> lines = null;
        while (deadline.hasTimeLeft()) {
            if (Files.exists(resultPath)) {
                lines = readResultFiles(resultPath);
                if (lines.size() == resultSize) {
                    success = true;
                    assertThat(lines).hasSameElementsAs(items);
                    break;
                } else {
                    LOG.info(
                            "The target result file {} does not contain enough records, current {} "
                                    + "records, left time: {}s",
                            resultPath,
                            lines.size(),
                            deadline.timeLeft().getSeconds());
                }
            } else {
                LOG.info("The target Json {} does not exist now", resultPath);
            }
            Thread.sleep(500);
        }
        assertTrue(
                success,
                String.format(
                        "Did not get expected results before timeout, actual result: %s.", lines));
    }

    private List<String> initializeSqlLines(String sqlPath, Map<String, String> vars)
            throws IOException {
        URL url = HiveITCase.class.getClassLoader().getResource(sqlPath);
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

    private void executeSqlStatements(ClusterController clusterController, List<String> sqlLines)
            throws Exception {
        clusterController.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines).addJar(sqlHiveJar).build(),
                Duration.ofMinutes(2L));
    }

    private static FlinkResource buildFlinkResource() {
        FlinkResourceSetup.FlinkResourceSetupBuilder builder =
                FlinkResourceSetup.builder().addJar(sqlHiveJar, JarLocation.LIB);

        // add hadoop jars
        File hadoopClasspathFile = new File(HADOOP_CLASS_PATH.toAbsolutePath().toString());
        if (!hadoopClasspathFile.exists()) {
            throw new RuntimeException(
                    "File that contains hadoop classpath "
                            + HADOOP_CLASS_PATH
                            + " does not exist.");
        }
        try {
            String classPathContent = FileUtils.readFileUtf8(hadoopClasspathFile);
            Arrays.stream(classPathContent.split(":"))
                    .map(Paths::get)
                    .forEach(jar -> builder.addJar(jar, JarLocation.LIB));
        } catch (Exception e) {
            throw new RuntimeException("Failed to build the FlinkResource.", e);
        }

        return new LocalStandaloneFlinkResourceFactory().create(builder.build());
    }

    private static File createHiveConfFile() {
        HiveConf hiveConf = new HiveConf();
        try (InputStream inputStream =
                Files.newInputStream(
                        new File(
                                        Objects.requireNonNull(
                                                        HiveITCase.class
                                                                .getClassLoader()
                                                                .getResource(
                                                                        HiveCatalog.HIVE_SITE_FILE))
                                                .toURI())
                                .toPath())) {
            hiveConf.addResource(inputStream, HiveCatalog.HIVE_SITE_FILE);
            // trigger a read from the conf so that the input stream is read
            hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load hive-site.xml from specified path", e);
        }
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, HIVE_CONTAINER.getHiveMetastoreURL());
        try {
            File site = TMP_FOLDER.newFile(HiveCatalog.HIVE_SITE_FILE);
            try (OutputStream out = Files.newOutputStream(site.toPath())) {
                hiveConf.writeXml(out);
            }
            return site;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create hive conf.", e);
        }
    }
}
