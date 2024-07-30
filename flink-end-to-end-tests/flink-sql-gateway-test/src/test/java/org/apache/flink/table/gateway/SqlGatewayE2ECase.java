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
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.endpoint.hive.HiveServer2Endpoint;
import org.apache.flink.table.gateway.containers.HiveContainer;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobClientMode;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkDistribution;
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
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.CATALOG_HIVE_CONF_DIR;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_PORT;
import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions.ADDRESS;
import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions.PORT;
import static org.apache.flink.table.utils.DateTimeUtils.formatTimestampMillis;
import static org.apache.flink.tests.util.TestUtils.readCsvResultFiles;
import static org.junit.Assert.assertEquals;

/** E2E Tests for {@code SqlGateway} with {@link HiveServer2Endpoint}. */
public class SqlGatewayE2ECase extends TestLogger {

    private static final Path HIVE_SQL_CONNECTOR_JAR =
            ResourceTestUtils.getResource(".*dependencies/flink-sql-connector-hive-.*.jar");
    private static final Path TEST_FILESYSTEM_CONNECTOR_JAR =
            ResourceTestUtils.getResource(
                    ".*dependencies/flink-table-filesystem-test-utils-.*.jar");
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

    private static final String CATALOG_NAME_PREFIX = "filesystem_catalog_";

    private static final String FILESYSTEM_DEFAULT_DATABASE = "test_db";
    private static final AtomicInteger CATALOG_COUNTER = new AtomicInteger(0);
    private static String filesystemCatalogName;

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

    @Test
    public void testMaterializedTableInContinuousMode() throws Exception {
        Duration continuousWaitTime = Duration.ofMinutes(5);
        Duration continuousWaitPause = Duration.ofSeconds(10);

        try (GatewayController gateway = flinkResource.startSqlGateway();
                ClusterController ignore = flinkResource.startCluster(2)) {

            FlinkDistribution.TestSqlGatewayRestClient gatewayRestClient =
                    initSessionWithCatalogStore(Collections.emptyMap());

            gatewayRestClient.executeStatementWithResult(
                    "CREATE TABLE streaming_source (\n"
                            + "    `timestamp` TIMESTAMP(3),\n"
                            + "    `user` VARCHAR,\n"
                            + "    `type` VARCHAR\n"
                            + " ) with ("
                            + "   'format' = 'json',"
                            + "   'source.monitor-interval' = '10s'"
                            + ")");

            gatewayRestClient.executeStatementWithResult(
                    "insert into streaming_source select TO_TIMESTAMP('2024-06-20 00:00:00'), 'Alice', 'INFO'");
            gatewayRestClient.executeStatementWithResult(
                    "insert into streaming_source select TO_TIMESTAMP('2024-06-20 00:00:00'), 'Bob', 'ERROR'");

            gatewayRestClient.executeStatementWithResult(
                    " CREATE MATERIALIZED TABLE my_materialized_table_in_continuous_mode\n"
                            + " PARTITIONED BY (ds)\n"
                            + " with (\n"
                            + "   'format' = 'json',\n"
                            + "   'sink.rolling-policy.rollover-interval' = '10s',\n"
                            + "   'sink.rolling-policy.check-interval' = '10s'\n"
                            + "  )\n"
                            + " FRESHNESS = INTERVAL '10' SECOND\n"
                            + " REFRESH_MODE = CONTINUOUS\n"
                            + " AS SELECT\n"
                            + " DATE_FORMAT(`timestamp`, 'yyyy-MM-dd') AS ds,\n"
                            + " user,\n"
                            + " type\n"
                            + " FROM streaming_source");

            // set current session mode to batch for verify the materialized table
            gatewayRestClient.executeStatementWithResult("SET 'execution.runtime-mode' = 'batch'");

            // verify the result
            CommonTestUtils.waitUntilIgnoringExceptions(
                    () -> {
                        List<RowData> result =
                                gatewayRestClient.executeStatementWithResult(
                                        "select * from my_materialized_table_in_continuous_mode order by ds, user");
                        return result.toString()
                                .equals("[+I(2024-06-20,Alice,INFO), +I(2024-06-20,Bob,ERROR)]");
                    },
                    continuousWaitTime,
                    continuousWaitPause,
                    "Failed to wait for the result");

            File savepointFolder = FOLDER.newFolder("savepoint");
            // configure savepoint path
            gatewayRestClient.executeStatementWithResult(
                    String.format(
                            "set 'execution.checkpointing.savepoint-dir'='file://%s'",
                            savepointFolder.getAbsolutePath()));

            // suspend the materialized table
            gatewayRestClient.executeStatementWithResult(
                    "ALTER MATERIALIZED TABLE my_materialized_table_in_continuous_mode SUSPEND");

            // send more data to the source
            gatewayRestClient.executeStatementWithResult(
                    "insert into streaming_source select TO_TIMESTAMP('2024-06-20 00:00:00'), 'Charlie', 'WARN'");

            // resume the materialized table
            gatewayRestClient.executeStatementWithResult(
                    "ALTER MATERIALIZED TABLE my_materialized_table_in_continuous_mode RESUME");

            // verify the result
            CommonTestUtils.waitUntilIgnoringExceptions(
                    () -> {
                        List<RowData> result =
                                gatewayRestClient.executeStatementWithResult(
                                        "select * from my_materialized_table_in_continuous_mode order by ds, user");
                        return result.toString()
                                .equals(
                                        "[+I(2024-06-20,Alice,INFO), +I(2024-06-20,Bob,ERROR), +I(2024-06-20,Charlie,WARN)]");
                    },
                    continuousWaitTime,
                    continuousWaitPause,
                    "Failed to wait for the result");

            // drop the materialized table
            gatewayRestClient.executeStatementWithResult(
                    "DROP MATERIALIZED TABLE my_materialized_table_in_continuous_mode");
        }
    }

    @Test
    public void testMaterializedTableInFullMode() throws Exception {
        Duration fullModeWaitTime = Duration.ofMinutes(5);
        Duration fullModeWaitPause = Duration.ofSeconds(10);

        // init session
        try (GatewayController gateway = flinkResource.startSqlGateway();
                ClusterController ignore = flinkResource.startCluster(2)) {

            Map<String, String> sessionProperties = new HashMap<>();
            sessionProperties.put("workflow-scheduler.type", "embedded");
            FlinkDistribution.TestSqlGatewayRestClient gatewayRestClient =
                    initSessionWithCatalogStore(sessionProperties);

            gatewayRestClient.executeStatementWithResult(
                    "CREATE TABLE batch_source (\n"
                            + "    `timestamp` TIMESTAMP(3),\n"
                            + "    `user` VARCHAR,\n"
                            + "    `type` VARCHAR\n"
                            + " ) with ("
                            + "   'format' = 'json'"
                            + ")");

            gatewayRestClient.executeStatementWithResult(
                    " CREATE MATERIALIZED TABLE my_materialized_table_in_full_mode\n"
                            + " PARTITIONED BY (ds)\n"
                            + " WITH (\n"
                            + "   'partition.fields.ds.date-formatter' = 'yyyy-MM-dd',\n"
                            + "   'format' = 'json'\n"
                            + " )\n"
                            + " FRESHNESS = INTERVAL '1' MINUTE\n"
                            + " REFRESH_MODE = FULL\n"
                            + " AS SELECT\n"
                            + " ds,\n"
                            + " count(*) as cnt\n"
                            + " FROM (\n"
                            + "   SELECT\n"
                            + "   DATE_FORMAT(`timestamp`, 'yyyy-MM-dd') AS ds,\n"
                            + "   user,\n"
                            + " type\n"
                            + " FROM batch_source\n"
                            + " ) GROUP BY ds");

            long systemTime = System.currentTimeMillis();
            String todayTimestamp =
                    formatTimestampMillis(systemTime, "yyyy-MM-dd HH:mm:ss", TimeZone.getDefault());
            String yesterdayTimestamp =
                    formatTimestampMillis(
                            systemTime - 24 * 60 * 60 * 1000,
                            "yyyy-MM-dd HH:mm:ss",
                            TimeZone.getDefault());
            String tomorrowTimestamp =
                    formatTimestampMillis(
                            systemTime + 24 * 60 * 60 * 1000,
                            "yyyy-MM-dd HH:mm:ss",
                            TimeZone.getDefault());
            String todayDateStr = todayTimestamp.substring(0, 10);
            String yesterdayDateStr = yesterdayTimestamp.substring(0, 10);
            String tomorrowDateStr = tomorrowTimestamp.substring(0, 10);

            // Both send date to current date, yesterday and tomorrow
            gatewayRestClient.executeStatementWithResult(
                    String.format(
                            "INSERT INTO batch_source VALUES "
                                    + "(TO_TIMESTAMP('%s'), 'Alice', 'INFO'), "
                                    + "(TO_TIMESTAMP('%s'), 'Alice', 'INFO'), "
                                    + "(TO_TIMESTAMP('%s'), 'Alice', 'INFO')",
                            yesterdayTimestamp, todayTimestamp, tomorrowTimestamp));

            // set current session mode to batch for verify the materialized table
            gatewayRestClient.executeStatementWithResult("SET 'execution.runtime-mode' = 'batch'");

            // verify the materialized table should auto refresh the today partition or tomorrow
            // partition
            CommonTestUtils.waitUntilIgnoringExceptions(
                    () -> {
                        List<RowData> result =
                                gatewayRestClient.executeStatementWithResult(
                                        "select * from my_materialized_table_in_full_mode order by ds");
                        String resultStr = result.toString();
                        return (resultStr.contains(String.format("+I(%s,1)", todayDateStr))
                                        || resultStr.contains(
                                                String.format("+I(%s,1)", tomorrowDateStr)))
                                && (!resultStr.contains(
                                        String.format("+I(%s,1)", yesterdayDateStr)));
                    },
                    fullModeWaitTime,
                    fullModeWaitPause,
                    "Failed to wait for the materialized table result");

            // suspend the materialized table
            gatewayRestClient.executeStatementWithResult(
                    "ALTER MATERIALIZED TABLE my_materialized_table_in_full_mode SUSPEND");

            // insert more data to the batch_source table
            gatewayRestClient.executeStatementWithResult(
                    String.format(
                            "INSERT INTO batch_source VALUES "
                                    + "(TO_TIMESTAMP('%s'), 'Bob', 'INFO'), "
                                    + "(TO_TIMESTAMP('%s'), 'Bob', 'INFO'), "
                                    + "(TO_TIMESTAMP('%s'), 'Bob', 'INFO')",
                            yesterdayTimestamp, todayTimestamp, tomorrowTimestamp));

            // resume the materialized table
            gatewayRestClient.executeStatementWithResult(
                    "ALTER MATERIALIZED TABLE my_materialized_table_in_full_mode RESUME");

            // wait until the materialized table is updated and verify only today or tomorrow
            // data
            // should be updated
            CommonTestUtils.waitUntilIgnoringExceptions(
                    () -> {
                        List<RowData> result =
                                gatewayRestClient.executeStatementWithResult(
                                        "select * from my_materialized_table_in_full_mode order by ds");
                        String resultStr = result.toString();
                        return (resultStr.contains(String.format("+I(%s,2)", todayDateStr))
                                        || resultStr.contains(
                                                String.format("+I(%s,2)", tomorrowDateStr)))
                                && (!resultStr.contains(
                                        String.format("+I(%s,2)", yesterdayDateStr)));
                    },
                    fullModeWaitTime,
                    fullModeWaitPause,
                    "Failed to wait for the result");

            // manual refresh all partitions
            gatewayRestClient.executeStatementWithResult(
                    "ALTER MATERIALIZED TABLE my_materialized_table_in_full_mode REFRESH PARTITION (ds='"
                            + todayDateStr
                            + "')");
            gatewayRestClient.executeStatementWithResult(
                    "ALTER MATERIALIZED TABLE my_materialized_table_in_full_mode REFRESH PARTITION (ds='"
                            + yesterdayDateStr
                            + "')");
            gatewayRestClient.executeStatementWithResult(
                    "ALTER MATERIALIZED TABLE my_materialized_table_in_full_mode REFRESH PARTITION (ds='"
                            + tomorrowDateStr
                            + "')");

            // verify the materialized table that all partitions are updated
            CommonTestUtils.waitUntilIgnoringExceptions(
                    () -> {
                        List<RowData> result =
                                gatewayRestClient.executeStatementWithResult(
                                        "select * from my_materialized_table_in_full_mode order by ds");
                        return result.toString()
                                .equals(
                                        String.format(
                                                "[+I(%s,2), +I(%s,2), +I(%s,2)]",
                                                yesterdayDateStr, todayDateStr, tomorrowDateStr));
                    },
                    fullModeWaitTime,
                    fullModeWaitPause,
                    "Failed to wait for the result");

            // drop the materialized table
            gatewayRestClient.executeStatementWithResult(
                    "DROP MATERIALIZED TABLE my_materialized_table_in_full_mode");
        }
    }

    private FlinkDistribution.TestSqlGatewayRestClient initSessionWithCatalogStore(
            Map<String, String> extraProperties) throws Exception {
        File catalogStoreFolder = FOLDER.newFolder();
        Map<String, String> sessionProperties = new HashMap<>();
        sessionProperties.put("table.catalog-store.kind", "file");
        sessionProperties.put(
                "table.catalog-store.file.path", catalogStoreFolder.getAbsolutePath());
        sessionProperties.putAll(extraProperties);

        FlinkDistribution.TestSqlGatewayRestClient gatewayRestClient =
                new FlinkDistribution.TestSqlGatewayRestClient(
                        InetAddress.getByName("localhost").getHostAddress(),
                        restPort.getPort(),
                        SqlGatewayRestAPIVersion.getDefaultVersion().toString().toLowerCase(),
                        sessionProperties);

        filesystemCatalogName = CATALOG_NAME_PREFIX + CATALOG_COUNTER.getAndAdd(1);
        File catalogFolder = FOLDER.newFolder(filesystemCatalogName);
        FOLDER.newFolder(
                String.format("%s/%s", filesystemCatalogName, FILESYSTEM_DEFAULT_DATABASE));
        String createCatalogDDL =
                String.format(
                        "CREATE CATALOG %s WITH (\n"
                                + "  'type' = 'test-filesystem',\n"
                                + "  'default-database' = 'test_db',\n"
                                + "  'path' = '%s'\n"
                                + ")",
                        filesystemCatalogName, catalogFolder.getAbsolutePath());
        gatewayRestClient.executeStatementWithResult(createCatalogDDL);
        gatewayRestClient.executeStatementWithResult(
                String.format("USE CATALOG %s", filesystemCatalogName));

        return gatewayRestClient;
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
                        .addJar(TEST_FILESYSTEM_CONNECTOR_JAR, JarLocation.LIB)
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
