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

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FileUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the dynamic partition pruning optimization on Hive sources. */
@ExtendWith(ParameterizedTestExtension.class)
public class HiveDynamicPartitionPruningITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    @Parameter public boolean enableAdaptiveBatchScheduler;

    @Parameters(name = "enableAdaptiveBatchScheduler={0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    private TableEnvironment tableEnv;
    private HiveCatalog hiveCatalog;
    private String warehouse;

    @BeforeEach
    public void setup() {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog
                .getHiveConf()
                .setBoolVar(
                        HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES, false);
        hiveCatalog.open();
        warehouse = hiveCatalog.getHiveConf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);

        if (enableAdaptiveBatchScheduler) {
            tableEnv = HiveTestUtils.createTableEnvInBatchModeWithAdaptiveScheduler();
        } else {
            tableEnv = HiveTestUtils.createTableEnvInBatchMode();
        }

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
    }

    @AfterEach
    public void tearDown() {
        if (hiveCatalog != null) {
            hiveCatalog.close();
        }
        if (warehouse != null) {
            FileUtils.deleteDirectoryQuietly(new File(warehouse));
        }
    }

    @TestTemplate
    public void testDynamicPartitionPruning() throws Exception {
        // src table
        tableEnv.executeSql("create table dim (x int,y string,z int)");
        tableEnv.executeSql("insert into dim values (1,'a',1),(2,'b',1),(3,'c',2)").await();

        // partitioned dest table
        tableEnv.executeSql("create table fact (a int, b bigint, c string) partitioned by (p int)");
        tableEnv.executeSql(
                        "insert into fact partition (p=1) values (10,100,'aaa'),(11,101,'bbb'),(12,102,'ccc') ")
                .await();
        tableEnv.executeSql(
                        "insert into fact partition (p=2) values (20,200,'aaa'),(21,201,'bbb'),(22,202,'ccc') ")
                .await();
        tableEnv.executeSql(
                        "insert into fact partition (p=3) values (30,300,'aaa'),(31,301,'bbb'),(32,302,'ccc') ")
                .await();

        System.out.println(
                tableEnv.explainSql(
                        "select a, b, c, p, x, y from fact, dim where x = p and z = 1 order by a"));

        tableEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM, false);

        String sql = "select a, b, c, p, x, y from fact, dim where x = p and z = 1 order by a";
        String sqlSwapFactDim =
                "select a, b, c, p, x, y from dim, fact where x = p and z = 1 order by a";

        String expected =
                "[+I[10, 100, aaa, 1, 1, a], +I[11, 101, bbb, 1, 1, a], +I[12, 102, ccc, 1, 1, a], "
                        + "+I[20, 200, aaa, 2, 2, b], +I[21, 201, bbb, 2, 2, b], +I[22, 202, ccc, 2, 2, b]]";

        // Check dynamic partition pruning is working
        String plan = tableEnv.explainSql(sql);
        assertThat(plan).contains("DynamicFilteringDataCollector");

        plan = tableEnv.explainSql(sqlSwapFactDim);
        assertThat(plan).contains("DynamicFilteringDataCollector");

        // Validate results
        List<Row> results = queryResult(tableEnv.sqlQuery(sql));
        assertThat(results.toString()).isEqualTo(expected);

        results = queryResult(tableEnv.sqlQuery(sqlSwapFactDim));
        assertThat(results.toString()).isEqualTo(expected);

        // Validate results with table statistics
        tableEnv.getCatalog(tableEnv.getCurrentCatalog())
                .get()
                .alterTableStatistics(
                        new ObjectPath(tableEnv.getCurrentDatabase(), "dim"),
                        new CatalogTableStatistics(3, -1, -1, -1),
                        false);

        results = queryResult(tableEnv.sqlQuery(sql));
        assertThat(results.toString()).isEqualTo(expected);

        results = queryResult(tableEnv.sqlQuery(sqlSwapFactDim));
        assertThat(results.toString()).isEqualTo(expected);
    }

    @TestTemplate
    public void testDynamicPartitionPruningOnTwoFactTables() throws Exception {
        tableEnv.executeSql("create table dim (x int,y string,z int)");
        tableEnv.executeSql("insert into dim values (1,'a',1),(2,'b',1),(3,'c',2)").await();

        // partitioned dest table
        tableEnv.executeSql("create table fact (a int, b bigint, c string) partitioned by (p int)");
        tableEnv.executeSql(
                        "insert into fact partition (p=1) values (10,100,'aaa'),(11,101,'bbb'),(12,102,'ccc') ")
                .await();
        tableEnv.executeSql(
                        "insert into fact partition (p=2) values (20,200,'aaa'),(21,201,'bbb'),(22,202,'ccc') ")
                .await();
        tableEnv.executeSql(
                        "insert into fact partition (p=3) values (30,300,'aaa'),(31,301,'bbb'),(32,302,'ccc') ")
                .await();

        // partitioned dest table
        tableEnv.executeSql(
                "create table fact2 (a int, b bigint, c string) partitioned by (p int)");
        tableEnv.executeSql(
                        "insert into fact2 partition (p=1) values (40,100,'aaa'),(41,101,'bbb'),(42,102,'ccc') ")
                .await();
        tableEnv.executeSql(
                        "insert into fact2 partition (p=2) values (50,200,'aaa'),(51,201,'bbb'),(52,202,'ccc') ")
                .await();
        tableEnv.executeSql(
                        "insert into fact2 partition (p=3) values (60,300,'aaa'),(61,301,'bbb'),(62,302,'ccc') ")
                .await();

        tableEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM, false);

        // two fact sources share the same dynamic filter
        String sql =
                "select * from ((select a, b, c, p, x, y from fact, dim where x = p and z = 1) "
                        + "union all "
                        + "(select a, b, c, p, x, y from fact2, dim where x = p and z = 1)) t order by a";
        String expected =
                "[+I[10, 100, aaa, 1, 1, a], +I[11, 101, bbb, 1, 1, a], +I[12, 102, ccc, 1, 1, a], "
                        + "+I[20, 200, aaa, 2, 2, b], +I[21, 201, bbb, 2, 2, b], +I[22, 202, ccc, 2, 2, b], "
                        + "+I[40, 100, aaa, 1, 1, a], +I[41, 101, bbb, 1, 1, a], +I[42, 102, ccc, 1, 1, a], "
                        + "+I[50, 200, aaa, 2, 2, b], +I[51, 201, bbb, 2, 2, b], +I[52, 202, ccc, 2, 2, b]]";

        String plan = tableEnv.explainSql(sql);
        assertThat(plan).containsOnlyOnce("DynamicFilteringDataCollector(fields=[x])(reuse_id=");

        List<Row> results = queryResult(tableEnv.sqlQuery(sql));
        assertThat(results.toString()).isEqualTo(expected);

        // two fact sources use different dynamic filters
        String sql2 =
                "select * from ((select a, b, c, p, x, y from fact, dim where x = p and z = 1) "
                        + "union all "
                        + "(select a, b, c, p, x, y from fact2, dim where x = p and z = 2)) t order by a";
        String expected2 =
                "[+I[10, 100, aaa, 1, 1, a], +I[11, 101, bbb, 1, 1, a], +I[12, 102, ccc, 1, 1, a], "
                        + "+I[20, 200, aaa, 2, 2, b], +I[21, 201, bbb, 2, 2, b], +I[22, 202, ccc, 2, 2, b], "
                        + "+I[60, 300, aaa, 3, 3, c], +I[61, 301, bbb, 3, 3, c], +I[62, 302, ccc, 3, 3, c]]";

        plan = tableEnv.explainSql(sql2);
        assertThat(plan).contains("DynamicFilteringDataCollector");

        results = queryResult(tableEnv.sqlQuery(sql2));
        assertThat(results.toString()).isEqualTo(expected2);
    }

    private static List<Row> queryResult(org.apache.flink.table.api.Table table) {
        return CollectionUtil.iteratorToList(table.execute().collect());
    }
}
