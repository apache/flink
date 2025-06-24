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

package org.apache.flink.table.planner.plan.optimize;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesCatalog;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;

/** Test optimized plans for different shuffle mode. */
@ExtendWith(ParameterizedTestExtension.class)
class ShuffleModePlanOptimizeTest extends TableTestBase {

    @Parameters(name = "mode = {0}")
    public static Collection<BatchShuffleMode> parameters() {
        return Arrays.asList(
                BatchShuffleMode.ALL_EXCHANGES_BLOCKING,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL);
    }

    @Parameter public BatchShuffleMode mode;

    private final BatchTableTestUtil util = batchTestUtil(TableConfig.getDefault());
    private final TestValuesCatalog catalog =
            new TestValuesCatalog("testCatalog", "test_database", true);

    @BeforeEach
    void setup() {
        catalog.open();
        util.tableEnv().registerCatalog("testCatalog", catalog);
        util.tableEnv().useCatalog("testCatalog");
        TableConfig tableConfig = util.tableEnv().getConfig();
        tableConfig.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED, true);
        if (mode != null) {
            tableConfig.set(ExecutionOptions.BATCH_SHUFFLE_MODE, mode);
        }

        // partition fact table.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE fact_part (\n"
                                + "  id BIGINT,\n"
                                + "  name STRING,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT,\n"
                                + "  fact_date_sk BIGINT\n"
                                + ") PARTITIONED BY (fact_date_sk)\n"
                                + "WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'runtime-source' = 'NewSource',\n"
                                + " 'partition-list' = 'fact_date_sk:1990;fact_date_sk:1991;fact_date_sk:1992',\n"
                                + " 'dynamic-filtering-fields' = 'fact_date_sk;amount',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        // dim table.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE dim (\n"
                                + "  id BIGINT,\n"
                                + "  male BOOLEAN,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT,\n"
                                + "  dim_date_sk BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'runtime-source' = 'NewSource',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
    }

    @TestTemplate
    void testMultipleInputWithDPP() {
        String query =
                "SELECT * FROM"
                        + " (Select count(*) c1 from fact_part, dim "
                        + "where fact_part.fact_date_sk = dim_date_sk and fact_part.price < 100) s1,"
                        + " (Select count(*) c2 from fact_part, dim "
                        + "where fact_part.fact_date_sk = dim_date_sk and dim.price < 200) s2,"
                        + " (Select count(*) c3 from fact_part, dim "
                        + "where fact_part.fact_date_sk = dim_date_sk and dim.price < 400) s3";
        util.verifyExecPlan(query);
    }

    @TestTemplate
    void testMultipleInputWithoutDPP() {
        String query =
                "SELECT * FROM"
                        + " (Select count(*) c1 from fact_part, dim "
                        + "where fact_part.fact_date_sk = dim_date_sk and fact_part.price < 100) s1,"
                        + " (Select count(*) c2 from fact_part, dim "
                        + "where fact_part.fact_date_sk = dim_date_sk and fact_part.price < 200) s2,"
                        + " (Select count(*) c3 from fact_part, dim "
                        + "where fact_part.fact_date_sk = dim_date_sk and fact_part.price < 400) s3";
        util.verifyExecPlan(query);
    }
}
