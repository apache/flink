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

package org.apache.flink.table.tpch;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.test.util.SQLJobSubmission;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class TpchITCase {
    private static final Logger LOGGER = LoggerFactory.getLogger(TpchITCase.class);
    private static final Network NETWORK = Network.newNetwork();
    private static final String dataPathInContainer = "/tmp/tpch/";
    private static final String SCALE = "0.01";

    @RegisterExtension
    static final FlinkContainers FLINK =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(
                            FlinkContainersSettings.builder()
                                    .numTaskManagers(1)
                                    .setConfigOption(
                                            TaskManagerOptions.MANAGED_MEMORY_FRACTION, 0.55f)
                                    .setConfigOption(
                                            BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED,
                                            false)
                                    .build())
                    .withTestcontainersSettings(
                            TestcontainersSettings.builder().network(NETWORK).build())
                    .build();

    @TempDir static Path tpchDirectory;
    static TpchQueries tpch;
    static String sourceTables;
    static String[] sinkTables;

    @BeforeAll
    static void beforeAll() throws Exception {
        LOGGER.info("Generating TPC-H dataset in {}", tpchDirectory);
        TpchDataGenerator.main(new String[] {SCALE, tpchDirectory.toString()});
        copyDataFiles();
        tpch = new TpchQueries(tpchDirectory);
        sourceTables = tpch.getSourceTables(dataPathInContainer + "/table");
        sinkTables = tpch.getSinkTables(dataPathInContainer + "/result");
    }

    @ParameterizedTest
    @ValueSource(
            ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22})
    void testTpch(final int queryNum) throws Exception {
        final List<String> sqlQuery =
                Arrays.asList( //
                        "SET execution.runtime-mode=batch;",
                        "SET parallelism.default=2;",
                        sourceTables,
                        sinkTables[queryNum],
                        "",
                        "INSERT INTO q" + queryNum + " " + tpch.getQuery(queryNum) + ";"
                        //
                        );
        final JobID jobId =
                FLINK.submitSQLJob(new SQLJobSubmission.SQLJobSubmissionBuilder(sqlQuery).build());
        LOGGER.info("Waiting for job with jobId '{}' to finish.", jobId);
        FLINK.waitUntilJobStatus(jobId, JobStatus.FINISHED);
        final String actual =
                FLINK.getOutputPathContent(
                        dataPathInContainer + "/result/q" + queryNum, "part-*", false);
        final String expected = tpch.getExpectedResult(queryNum);
        assertThat(validateResult(expected, actual)).isTrue();
    }

    private boolean validateResult(final String expected, final String actual) {
        final String[] expectedLines = expected.split("\n");
        final String[] actualLines = actual.split("\n");
        if (actualLines.length != expectedLines.length) {
            LOGGER.info("Expected and actual lines do not match.");
            return false;
        }
        for (int i = 0; i < actualLines.length; i++) {
            final String[] expectedColumns = expectedLines[i].split("\\|");
            final String[] actualColumns = actualLines[i].split("\\|");
            for (int j = 0; j < actualColumns.length; j++) {
                if (!TpchResultComparator.validateColumn(expectedColumns[j], actualColumns[j])) {
                    LOGGER.info(
                            "Expected column '{}' does not match actual '{}' on line {}.",
                            expectedColumns[j],
                            actualColumns[j],
                            i);
                    return false;
                }
            }
        }
        return true;
    }

    private static void copyDataFiles() {
        copyTpchData(FLINK.getJobManager());
        FLINK.getTaskManagers().forEach(TpchITCase::copyTpchData);
    }

    private static void copyTpchData(final GenericContainer<?> container) {
        final Path table = tpchDirectory.resolve("table");
        try (final Stream<Path> stream = Files.walk(table)) {
            stream.filter(Files::isRegularFile)
                    .forEach(
                            path -> {
                                final Path filename = table.relativize(path);
                                final String pathInContainer =
                                        dataPathInContainer + "/table/" + filename;
                                LOGGER.info(
                                        "Copying file {} into JobManager path {}",
                                        path,
                                        pathInContainer);
                                container.copyFileToContainer(
                                        MountableFile.forHostPath(path), pathInContainer);
                            });
        } catch (final IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    /** Helper class for TPC-H queries. */
    private static class TpchQueries {
        private static final String MODIFIED_QUERIES_PATH = "tpch/modified-query";
        private final Path datasetPath;

        public TpchQueries(final Path datasetPath) {
            this.datasetPath = datasetPath;
        }

        public String getQuery(final int queryNum) throws IOException {
            if (isModifiedQuery(queryNum)) {
                return readResource(MODIFIED_QUERIES_PATH + "/q" + queryNum + ".sql");
            }
            final Path queryPath = datasetPath.resolve("query/" + "q" + queryNum + ".sql");
            return new String(Files.readAllBytes(queryPath));
        }

        public boolean isModifiedQuery(final int queryNum) {
            return queryNum == 6 || queryNum == 11 || queryNum == 15 || queryNum == 20;
        }

        public String getExpectedResult(final int queryNum) throws IOException {
            final Path path = datasetPath.resolve("expected/" + "q" + queryNum + ".csv");
            return new String(Files.readAllBytes(path));
        }

        private String readResource(final String resourceName) {
            try (final InputStream resourceAsStream =
                    getClass().getClassLoader().getResourceAsStream(resourceName)) {
                return readFromInputStream(Objects.requireNonNull(resourceAsStream));
            } catch (final IOException | NullPointerException exception) {
                throw new IllegalStateException(
                        "Failed to read from resource '" + resourceName + "'.", exception);
            }
        }

        private String readFromInputStream(final InputStream inputStream) {
            return new BufferedReader(new InputStreamReader(inputStream))
                    .lines()
                    .collect(Collectors.joining("\n"));
        }

        public String[] getSinkTables(final String resultPath) {
            return new String[] { //
                "zeroth query",
                // 01
                "CREATE TABLE q1 ("
                        + //
                        "       l_returnflag VARCHAR,"
                        + //
                        "       l_linestatus VARCHAR,"
                        + //
                        "       sum_qty DOUBLE,"
                        + //
                        "       sum_base_price DOUBLE,"
                        + //
                        "       sum_disc_price DOUBLE,"
                        + //
                        "       sum_charge DOUBLE,"
                        + //
                        "       avg_qty DOUBLE,"
                        + //
                        "       avg_price DOUBLE,"
                        + //
                        "       avg_disc DOUBLE,"
                        + //
                        "       count_order BIGINT"
                        + //
                        ") WITH ("
                        + //
                        "       'connector' = 'filesystem',"
                        + //
                        "       'path' = '"
                        + resultPath
                        + "/q1',"
                        + //
                        "       'format' = 'csv',"
                        + //
                        "       'csv.field-delimiter' = '|',"
                        + //
                        "       'sink.parallelism' = '1'"
                        + //
                        ");",
                // 02
                "CREATE TABLE q2("
                        + //
                        "       s_acctbal DOUBLE,"
                        + //
                        "       s_name VARCHAR,"
                        + //
                        "       n_name VARCHAR,"
                        + //
                        "       p_partkey BIGINT,"
                        + //
                        "       p_mfgr VARCHAR,"
                        + //
                        "       s_addres VARCHAR,"
                        + //
                        "       s_phone VARCHAR,"
                        + //
                        "       s_comment VARCHAR"
                        + //
                        ") WITH ("
                        + //
                        "       'connector' = 'filesystem',"
                        + //
                        "       'path' = '"
                        + resultPath
                        + "/q2',"
                        + //
                        "       'format' = 'csv',"
                        + //
                        "       'csv.field-delimiter' = '|',"
                        + //
                        "       'sink.parallelism' = '1'"
                        + //
                        ");",
                // 03
                "CREATE TABLE q3("
                        + //
                        "        l_orderkey BIGINT,"
                        + //
                        "        revenue DOUBLE,"
                        + //
                        "        o_orderdate DATE,"
                        + //
                        "        o_shippriority INT"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q3',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 04
                "CREATE TABLE q4("
                        + //
                        "        o_orderpriority VARCHAR,"
                        + //
                        "        order_count BIGINT"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q4',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 05
                "CREATE TABLE q5("
                        + //
                        "        n_name VARCHAR,"
                        + //
                        "        revenue DOUBLE"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q5',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 06
                "CREATE TABLE q6("
                        + //
                        "        revenue DOUBLE"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q6',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 07
                "CREATE TABLE q7("
                        + //
                        "        supp_nation VARCHAR,"
                        + //
                        "        cust_nation VARCHAR,"
                        + //
                        "        l_year BIGINT,"
                        + //
                        "        revenue DOUBLE"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q7',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 08
                "CREATE TABLE q8("
                        + //
                        "        o_year BIGINT,"
                        + //
                        "        mkt_share DOUBLE"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q8',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 09
                "CREATE TABLE q9("
                        + //
                        "        nation VARCHAR,"
                        + //
                        "        o_year BIGINT,"
                        + //
                        "        sum_profit DOUBLE"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q9',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 10
                "CREATE TABLE q10("
                        + //
                        "        c_custkey BIGINT,"
                        + //
                        "        c_name VARCHAR,"
                        + //
                        "        revenue DOUBLE,"
                        + //
                        "        c_acctbal DOUBLE,"
                        + //
                        "        n_name VARCHAR,"
                        + //
                        "        c_address VARCHAR,"
                        + //
                        "        c_phone VARCHAR,"
                        + //
                        "        c_comment VARCHAR"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q10',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 11
                "CREATE TABLE q11("
                        + //
                        "        ps_partkey BIGINT,"
                        + //
                        "        `value` DOUBLE"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q11',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 12
                "CREATE TABLE q12("
                        + //
                        "        l_shipmode VARCHAR,"
                        + //
                        "        high_line_count INT,"
                        + //
                        "        low_line_count INT"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q12',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 13
                "CREATE TABLE q13("
                        + //
                        "        c_count BIGINT,"
                        + //
                        "        custdist BIGINT"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q13',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 14
                "CREATE TABLE q14("
                        + //
                        "        promo_revenue DOUBLE"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q14',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 15
                "CREATE TABLE q15("
                        + //
                        "        s_suppkey BIGINT,"
                        + //
                        "        s_name VARCHAR,"
                        + //
                        "        s_address VARCHAR,"
                        + //
                        "        s_phone VARCHAR,"
                        + //
                        "        total_revenue DOUBLE"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q15',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 16
                "CREATE TABLE q16("
                        + //
                        "        p_brand VARCHAR,"
                        + //
                        "        p_type VARCHAR,"
                        + //
                        "        p_size INT,"
                        + //
                        "        supplier_cnt BIGINT"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q16',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 17
                "CREATE TABLE q17("
                        + //
                        "        avg_yearly DOUBLE"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q17',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 18
                "CREATE TABLE q18("
                        + //
                        "        c_name VARCHAR,"
                        + //
                        "        c_custkey BIGINT,"
                        + //
                        "        o_orderkey BIGINT,"
                        + //
                        "        o_orderdate DATE,"
                        + //
                        "        o_totalprice DOUBLE,"
                        + //
                        "        `sum(l_quantity)` DOUBLE"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q18',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 19
                "CREATE TABLE q19("
                        + //
                        "        revenue DOUBLE"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q19',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 20
                "CREATE TABLE q20("
                        + //
                        "        s_name VARCHAR,"
                        + //
                        "        s_address VARCHAR"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q20',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 21
                "CREATE TABLE q21("
                        + //
                        "        s_name VARCHAR,"
                        + //
                        "        numwait BIGINT"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q21',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");",
                // 22
                "CREATE TABLE q22("
                        + //
                        "        cntrycode VARCHAR,"
                        + //
                        "        numcust BIGINT,"
                        + //
                        "        totacctbal DOUBLE"
                        + //
                        ") WITH ("
                        + //
                        "        'connector' = 'filesystem',"
                        + //
                        "        'path' = '"
                        + resultPath
                        + "/q22',"
                        + //
                        "        'format' = 'csv',"
                        + //
                        "        'csv.field-delimiter' = '|',"
                        + //
                        "        'sink.parallelism' = '1'"
                        + //
                        ");"
            };
        }

        public String getSourceTables(final String dataPath) {
            return "CREATE TABLE customer ("
                    + //
                    "        c_custkey BIGINT,"
                    + //
                    "        c_name VARCHAR,"
                    + //
                    "        c_address VARCHAR,"
                    + //
                    "        c_nationkey BIGINT,"
                    + //
                    "        c_phone VARCHAR,"
                    + //
                    "        c_acctbal DOUBLE,"
                    + //
                    "        c_mktsegment VARCHAR,"
                    + //
                    "        c_comment VARCHAR"
                    + //
                    ") WITH ("
                    + //
                    "        'connector' = 'filesystem',"
                    + //
                    "        'path' = '"
                    + dataPath
                    + "/customer.csv',"
                    + //
                    "        'format' = 'csv',"
                    + //
                    "        'csv.field-delimiter' = '|',"
                    + //
                    "        'csv.allow-comments' = 'true'"
                    + //
                    ");"
                    + //
                    "\n"
                    + //
                    "CREATE TABLE lineitem ("
                    + //
                    "        l_orderkey BIGINT,"
                    + //
                    "        l_partkey BIGINT,"
                    + //
                    "        l_suppkey BIGINT,"
                    + //
                    "        l_linenumber INT,"
                    + //
                    "        l_quantity DOUBLE,"
                    + //
                    "        l_extendedprice DOUBLE,"
                    + //
                    "        l_discount DOUBLE,"
                    + //
                    "        l_tax DOUBLE,"
                    + //
                    "        l_returnflag VARCHAR,"
                    + //
                    "        l_linestatus VARCHAR,"
                    + //
                    "        l_shipdate DATE,"
                    + //
                    "        l_commitdate DATE,"
                    + //
                    "        l_receiptdate DATE,"
                    + //
                    "        l_shipinstruct VARCHAR,"
                    + //
                    "        l_shipmode VARCHAR,"
                    + //
                    "        l_comment VARCHAR"
                    + //
                    ") WITH ("
                    + //
                    "        'connector' = 'filesystem',"
                    + //
                    "        'path' = '"
                    + dataPath
                    + "/lineitem.csv',"
                    + //
                    "        'format' = 'csv',"
                    + //
                    "        'csv.field-delimiter' = '|',"
                    + //
                    "        'csv.allow-comments' = 'true'"
                    + //
                    ");"
                    + //
                    "\n"
                    + //
                    "CREATE TABLE nation ("
                    + //
                    "        n_nationkey BIGINT,"
                    + //
                    "        n_name VARCHAR,"
                    + //
                    "        n_regionkey BIGINT,"
                    + //
                    "        n_comment VARCHAR"
                    + //
                    ") WITH ("
                    + //
                    "        'connector' = 'filesystem',"
                    + //
                    "        'path' = '"
                    + dataPath
                    + "/nation.csv',"
                    + //
                    "        'format' = 'csv',"
                    + //
                    "        'csv.field-delimiter' = '|',"
                    + //
                    "        'csv.allow-comments' = 'true'"
                    + //
                    ");"
                    + //
                    "\n"
                    + //
                    "CREATE TABLE orders ("
                    + //
                    "        o_orderkey BIGINT,"
                    + //
                    "        o_custkey BIGINT,"
                    + //
                    "        o_orderstatus VARCHAR,"
                    + //
                    "        o_totalprice DOUBLE,"
                    + //
                    "        o_orderdate DATE,"
                    + //
                    "        o_orderpriority VARCHAR,"
                    + //
                    "        o_clerk VARCHAR,"
                    + //
                    "        o_shippriority INT,"
                    + //
                    "        o_comment VARCHAR"
                    + //
                    ") WITH ("
                    + //
                    "        'connector' = 'filesystem',"
                    + //
                    "        'path' ='"
                    + dataPath
                    + "/orders.csv',"
                    + //
                    "        'format' = 'csv',"
                    + //
                    "        'csv.field-delimiter' = '|',"
                    + //
                    "        'csv.allow-comments' = 'true'"
                    + //
                    ");"
                    + //
                    "\n"
                    + //
                    "CREATE TABLE part ("
                    + //
                    "        p_partkey BIGINT,"
                    + //
                    "        p_name VARCHAR,"
                    + //
                    "        p_mfgr VARCHAR,"
                    + //
                    "        p_brand VARCHAR,"
                    + //
                    "        p_type VARCHAR,"
                    + //
                    "        p_size INT,"
                    + //
                    "        p_container VARCHAR,"
                    + //
                    "        p_retailprice DOUBLE,"
                    + //
                    "        p_comment VARCHAR"
                    + //
                    ") WITH ("
                    + //
                    "        'connector' = 'filesystem',"
                    + //
                    "        'path' ='"
                    + dataPath
                    + "/part.csv',"
                    + //
                    "        'format' = 'csv',"
                    + //
                    "        'csv.field-delimiter' = '|',"
                    + //
                    "        'csv.allow-comments' = 'true'"
                    + //
                    ");"
                    + //
                    "\n"
                    + //
                    "CREATE TABLE partsupp ("
                    + //
                    "        ps_partkey BIGINT,"
                    + //
                    "        ps_suppkey BIGINT,"
                    + //
                    "        ps_availqty INT,"
                    + //
                    "        ps_supplycost DOUBLE,"
                    + //
                    "        ps_comment VARCHAR"
                    + //
                    ") WITH ("
                    + //
                    "        'connector' = 'filesystem',"
                    + //
                    "        'path' ='"
                    + dataPath
                    + "/partsupp.csv',"
                    + //
                    "        'format' = 'csv',"
                    + //
                    "        'csv.field-delimiter' = '|',"
                    + //
                    "        'csv.allow-comments' = 'true'"
                    + //
                    ");"
                    + //
                    "\n"
                    + //
                    "CREATE TABLE region("
                    + //
                    "        r_regionkey BIGINT,"
                    + //
                    "        r_name VARCHAR,"
                    + //
                    "        r_comment VARCHAR"
                    + //
                    ") WITH ("
                    + //
                    "        'connector' = 'filesystem',"
                    + //
                    "        'path' ='"
                    + dataPath
                    + "/region.csv',"
                    + //
                    "        'format' = 'csv',"
                    + //
                    "        'csv.field-delimiter' = '|',"
                    + //
                    "        'csv.allow-comments' = 'true'"
                    + //
                    ");"
                    + //
                    "\n"
                    + //
                    "CREATE TABLE supplier ("
                    + //
                    "        s_suppkey BIGINT,"
                    + //
                    "        s_name VARCHAR,"
                    + //
                    "        s_address VARCHAR,"
                    + //
                    "        s_nationkey BIGINT,"
                    + //
                    "        s_phone VARCHAR,"
                    + //
                    "        s_acctbal DOUBLE,"
                    + //
                    "        s_comment VARCHAR"
                    + //
                    ") WITH ("
                    + //
                    "        'connector' = 'filesystem',"
                    + //
                    "        'path' ='"
                    + dataPath
                    + "/supplier.csv',"
                    + //
                    "        'format' = 'csv',"
                    + //
                    "        'csv.field-delimiter' = '|',"
                    + //
                    "        'csv.allow-comments' = 'true'"
                    + //
                    ");"
                    + //
                    "\n";
        }
    }
}
