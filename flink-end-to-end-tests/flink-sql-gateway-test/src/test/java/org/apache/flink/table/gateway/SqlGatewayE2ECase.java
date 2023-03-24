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

package org.apache.flink.table.gateway;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.endpoint.hive.HiveServer2Endpoint;
import org.apache.flink.table.gateway.containers.HiveContainer;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobClientMode;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.GatewayController;
import org.apache.flink.tests.util.flink.JarLocation;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.CATALOG_HIVE_CONF_DIR;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_PORT;
import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions.ADDRESS;
import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions.PORT;
import static org.apache.flink.tests.util.TestUtils.readCsvResultFiles;
import static org.junit.Assert.assertEquals;

/** E2E Tests for {@code SqlGateway} with {@link HiveServer2Endpoint}. */
public class SqlGatewayE2ECase extends TestLogger {

    private static final Path HIVE_SQL_CONNECTOR_JAR =
            ResourceTestUtils.getResource(".*dependencies/flink-sql-connector-hive-.*.jar");
    private static final Path HADOOP_CLASS_PATH =
            ResourceTestUtils.getResource(".*hadoop.classpath");
    private static final String GATEWAY_E2E_SQL = "gateway_e2e.sql";
    private static final Configuration ENDPOINT_CONFIG = new Configuration();
    private static final String RESULT_KEY = "$RESULT";

    @ClassRule public static final TemporaryFolder FOLDER = new TemporaryFolder();
    @ClassRule public static final HiveContainer HIVE_CONTAINER = new HiveContainer();
    @Rule public final FlinkResource flinkResource = buildFlinkResource();

    private static NetUtils.Port hiveserver2Port;
    private static NetUtils.Port restPort;

    @BeforeClass
    public static void beforeClass() {
        ENDPOINT_CONFIG.setString(
                getPrefixedConfigOptionName(CATALOG_HIVE_CONF_DIR), createHiveConf().getParent());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        hiveserver2Port.close();
        restPort.close();
    }

    @Test
    public void testHiveServer2ExecuteStatement() throws Exception {
        executeStatement(
                SQLJobClientMode.getHiveJDBC(
                        InetAddress.getByName("localhost").getHostAddress(),
                        hiveserver2Port.getPort()));
    }

    @Test
    public void testRestExecuteStatement() throws Exception {
        executeStatement(
                SQLJobClientMode.getRestClient(
                        InetAddress.getByName("localhost").getHostAddress(),
                        restPort.getPort(),
                        SqlGatewayRestAPIVersion.getDefaultVersion().toString().toLowerCase()));
    }

    @Test
    public void testSqlClientExecuteStatement() throws Exception {
        executeStatement(
                SQLJobClientMode.getGatewaySqlClient(
                        InetAddress.getByName("localhost").getHostAddress(), restPort.getPort()));
    }

    private void executeStatement(SQLJobClientMode mode) throws Exception {
        File result = FOLDER.newFolder(mode.getClass().getName() + ".csv");
        try (GatewayController gateway = flinkResource.startSqlGateway();
                ClusterController ignore = flinkResource.startCluster(1)) {
            gateway.submitSQLJob(
                    new SQLJobSubmission.SQLJobSubmissionBuilder(getSqlLines(result))
                            .setClientMode(mode)
                            .build(),
                    Duration.ofSeconds(60));
        }
        assertEquals(Collections.singletonList("1"), readCsvResultFiles(result.toPath()));
    }

    private static List<String> getSqlLines(File result) throws Exception {
        URL url = SqlGatewayE2ECase.class.getClassLoader().getResource(GATEWAY_E2E_SQL);
        if (url == null) {
            throw new FileNotFoundException(GATEWAY_E2E_SQL);
        }
        String sql =
                Files.readAllLines(new File(url.getFile()).toPath()).stream()
                        .filter(line -> !line.trim().startsWith("--"))
                        .collect(Collectors.joining());
        return Arrays.stream(sql.split(";"))
                .map(line -> line.replace(RESULT_KEY, result.getAbsolutePath()) + ";")
                .collect(Collectors.toList());
    }

    private static File createHiveConf() {
        HiveConf hiveConf = new HiveConf();
        try (InputStream inputStream =
                new FileInputStream(
                        new File(
                                Objects.requireNonNull(
                                                SqlGatewayE2ECase.class
                                                        .getClassLoader()
                                                        .getResource(HiveCatalog.HIVE_SITE_FILE))
                                        .toURI()))) {
            hiveConf.addResource(inputStream, HiveCatalog.HIVE_SITE_FILE);
            // trigger a read from the conf so that the input stream is read
            hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load hive-site.xml from specified path", e);
        }
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, HIVE_CONTAINER.getHiveMetastoreURI());
        try {
            File site = FOLDER.newFile(HiveCatalog.HIVE_SITE_FILE);
            try (OutputStream out = new FileOutputStream(site)) {
                hiveConf.writeXml(out);
            }
            return site;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create hive conf.", e);
        }
    }

    /**
     * Build required environment. It prepares all hadoop jars to mock HADOOP_CLASSPATH, use
     * hadoop.classpath which contains all hadoop jars. It also moves planner to the lib and remove
     * the planner load to make the Hive sql connector works.
     */
    private static FlinkResource buildFlinkResource() {
        // add hive jar and planner jar
        FlinkResourceSetup.FlinkResourceSetupBuilder builder =
                FlinkResourceSetup.builder()
                        .addJar(HIVE_SQL_CONNECTOR_JAR, JarLocation.LIB)
                        .moveJar("flink-table-planner", JarLocation.OPT, JarLocation.LIB)
                        .moveJar("flink-table-planner-loader", JarLocation.LIB, JarLocation.OPT);
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
        // add configs for the following endpoints: hive server2, rest
        hiveserver2Port = NetUtils.getAvailablePort();
        restPort = NetUtils.getAvailablePort();
        Map<String, String> endpointConfig = new HashMap<>();
        endpointConfig.put("sql-gateway.endpoint.type", "hiveserver2;rest");
        // hive server2
        endpointConfig.put(
                getPrefixedConfigOptionName(THRIFT_PORT),
                String.valueOf(hiveserver2Port.getPort()));
        // rest
        endpointConfig.put(getRestPrefixedConfigOptionName(ADDRESS), "127.0.0.1");
        endpointConfig.put(
                getRestPrefixedConfigOptionName(PORT), String.valueOf(restPort.getPort()));

        ENDPOINT_CONFIG.addAll(Configuration.fromMap(endpointConfig));
        builder.addConfiguration(ENDPOINT_CONFIG);

        return new LocalStandaloneFlinkResourceFactory().create(builder.build());
    }

    private static String getPrefixedConfigOptionName(ConfigOption<?> option) {
        String prefix = "sql-gateway.endpoint.hiveserver2.";
        return prefix + option.key();
    }

    private static String getRestPrefixedConfigOptionName(ConfigOption<?> option) {
        String prefix = "sql-gateway.endpoint.rest.";
        return prefix + option.key();
    }
}
