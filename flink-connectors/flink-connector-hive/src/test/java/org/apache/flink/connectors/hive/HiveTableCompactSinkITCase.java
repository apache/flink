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

package org.apache.flink.connectors.hive;

import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** IT case for Hive table compaction in batch mode. */
@ExtendWith(TestLoggerExtension.class)
class HiveTableCompactSinkITCase {

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    private TableEnvironment tableEnv;
    private HiveCatalog hiveCatalog;
    private String warehouse;

    @BeforeEach
    void setUp() {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog.open();
        warehouse = hiveCatalog.getHiveConf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
        tableEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
    }

    @AfterEach
    void tearDown() {
        if (hiveCatalog != null) {
            hiveCatalog.close();
        }
    }

    @Test
    void testNoCompaction() throws Exception {
        tableEnv.executeSql(
                "CREATE TABLE src ("
                        + " key string,"
                        + " value string"
                        + ") TBLPROPERTIES ("
                        + " 'auto-compaction' = 'true', "
                        + " 'compaction.small-files.avg-size' = '1b', "
                        + " 'sink.parallelism' = '4'" // set sink parallelism = 4
                        + ")");
        tableEnv.executeSql(
                        "insert into src values ('k1', 'v1'), ('k2', 'v2'),"
                                + "('k3', 'v3'), ('k4', 'v4')")
                .await();

        List<Path> files = listDataFiles(Paths.get(warehouse, "src"));
        // auto compaction enabled, but the files' average size isn't less than 1b,  so the files
        // num should be 4
        assertThat(files).hasSize(4);
        List<String> result = toSortedResult(tableEnv.executeSql("select * from src"));
        assertThat(result.toString()).isEqualTo("[+I[k1, v1], +I[k2, v2], +I[k3, v3], +I[k4, v4]]");
    }

    @Test
    void testCompactNonPartitionedTable() throws Exception {
        tableEnv.executeSql(
                "CREATE TABLE src ("
                        + " key string,"
                        + " value string"
                        + ") TBLPROPERTIES ("
                        + " 'auto-compaction' = 'true', "
                        + " 'sink.parallelism' = '4'" // set sink parallelism = 4
                        + ")");
        tableEnv.executeSql(
                        "insert into src values ('k1', 'v1'), ('k2', 'v2'),"
                                + "('k3', 'v3'), ('k4', 'v4')")
                .await();

        // auto compaction is enabled, so all the files should be merged be one file
        List<Path> files = listDataFiles(Paths.get(warehouse, "src"));
        assertThat(files).hasSize(1);
        List<String> result = toSortedResult(tableEnv.executeSql("select * from src"));
        assertThat(result.toString()).isEqualTo("[+I[k1, v1], +I[k2, v2], +I[k3, v3], +I[k4, v4]]");
    }

    @Test
    void testCompactPartitionedTable() throws Exception {
        tableEnv.executeSql(
                "CREATE TABLE src ("
                        + " key string,"
                        + " value string"
                        + ") partitioned by (p1 int,p2 string) TBLPROPERTIES ("
                        + " 'auto-compaction' = 'true', "
                        + " 'sink.parallelism' = '8'" // set sink parallelism = 8
                        + ")");

        // test compaction for static partition
        tableEnv.executeSql(
                        "insert into src partition (p1=0,p2='static') values (1,'a'),(2,'b'),(3,'c')")
                .await();
        // auto compaction is enabled, so all the files in same partition should be merged be one
        // file
        List<Path> files = listDataFiles(Paths.get(warehouse, "src/p1=0/p2=static"));
        assertThat(files).hasSize(1);
        // verify the result
        List<String> result = toSortedResult(tableEnv.executeSql("select * from src"));
        assertThat(result.toString())
                .isEqualTo("[+I[1, a, 0, static], +I[2, b, 0, static], +I[3, c, 0, static]]");

        // test compaction for dynamic partition
        tableEnv.executeSql(
                        "insert into src partition (p1=0,p2) values (1,'a','d1'),"
                                + " (2,'b','d2'), (3,'c','d1'), (4,'d','d2')")
                .await();
        // auto compaction is enabled, so all the files in same partition should be merged be one
        // file
        files = listDataFiles(Paths.get(warehouse, "src/p1=0/p2=d1"));
        assertThat(files).hasSize(1);
        files = listDataFiles(Paths.get(warehouse, "src/p1=0/p2=d2"));
        assertThat(files).hasSize(1);
        // verify the result
        result = toSortedResult(tableEnv.executeSql("select * from src"));
        assertThat(result.toString())
                .isEqualTo(
                        "[+I[1, a, 0, d1], +I[1, a, 0, static], +I[2, b, 0, d2],"
                                + " +I[2, b, 0, static], +I[3, c, 0, d1], +I[3, c, 0, static], +I[4, d, 0, d2]]");
    }

    @Test
    void testConditionalCompact() throws Exception {
        tableEnv.executeSql(
                "CREATE TABLE src ("
                        + " key string,"
                        + " value string"
                        + ") partitioned by (p int) TBLPROPERTIES ("
                        + " 'auto-compaction' = 'true', "
                        + " 'compaction.small-files.avg-size' = '9b', "
                        + " 'sink.parallelism' = '4'" // set sink parallelism = 4
                        + ")");

        tableEnv.executeSql(
                        "insert into src values ('k1', 'v1', 1), ('k2', 'v2', 1),"
                                + "('k3', 'v3', 2), ('k4', 'v4', 2), ('k5', 'v5', 1)")
                .await();

        // one row is 6 bytes, so the partition "p=2" will contain two files, one of which only
        // contain one row, the average size is 6 bytes. so the files should be merged to one single
        // file.
        List<Path> files = listDataFiles(Paths.get(warehouse, "src/p=2"));
        assertThat(files).hasSize(1);

        // the partition "p=1" will contain two files, one contain two rows, and the other contain
        // one row. the average size is 9 bytes, so the files shouldn't be merged
        files = listDataFiles(Paths.get(warehouse, "src/p=1"));
        assertThat(files).hasSize(2);

        List<String> result = toSortedResult(tableEnv.executeSql("select * from src"));
        assertThat(result.toString())
                .isEqualTo(
                        "[+I[k1, v1, 1], +I[k2, v2, 1], +I[k3, v3, 2], +I[k4, v4, 2], +I[k5, v5, 1]]");
    }

    private List<Path> listDataFiles(Path path) throws Exception {
        String defaultSuccessFileName =
                FileSystemConnectorOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME.defaultValue();
        return Files.list(path)
                .filter(
                        p ->
                                !p.toFile().isHidden()
                                        && !p.toFile().getName().equals(defaultSuccessFileName))
                .collect(Collectors.toList());
    }

    private List<String> toSortedResult(TableResult tableResult) {
        List<Row> rows = CollectionUtil.iteratorToList(tableResult.collect());
        return rows.stream().map(Row::toString).sorted().collect(Collectors.toList());
    }
}
