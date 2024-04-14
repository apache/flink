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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;

/** Test rule {@link AggregateFilterPushdownRule}. */
@ExtendWith(ParameterizedTestExtension.class)
class AggregateFilterPushdownRuleTest extends TableTestBase {

    private static final String STREAM = "stream";
    private static final String BATCH = "batch";

    @Parameter private String mode;

    @Parameters(name = "mode = {0}")
    private static Collection<String> parameters() {
        return Arrays.asList(STREAM, BATCH);
    }

    private TableTestUtil util;

    @BeforeEach
    void setup() {
        boolean isStreaming = STREAM.equals(mode);
        if (isStreaming) {
            util = streamTestUtil(TableConfig.getDefault());
        } else {
            util = batchTestUtil(TableConfig.getDefault());
        }

        TableEnvironment tEnv = util.getTableEnv();
        String src =
                String.format(
                        "CREATE TABLE MyTable (\n"
                                + "  id int,\n"
                                + "  name varchar,\n"
                                + "  age int\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'filterable-fields' = 'id;name;age;',\n"
                                + "  'bounded' = '%s')",
                        !isStreaming);
        tEnv.executeSql(src);
    }

    @TestTemplate
    void testSingleFilterShouldPushdown() {
        util.verifyExecPlan("SELECT count(*) filter (where id > 1) FROM MyTable");
    }

    @TestTemplate
    void testMultiFiltersShouldPushdown() {
        util.verifyExecPlan("SELECT count(*) filter (where id > 1) FROM MyTable where age < 3");
    }

    @TestTemplate
    void testMultiFiltersShouldNotPushdown() {
        util.verifyExecPlan(
                "SELECT count(*) filter (where id > 1), sum(age) filter (where id > 5) FROM MyTable");
    }

    @TestTemplate
    void testSingleFilterMultiAggregagesShouldNotPushdown() {
        util.verifyExecPlan("SELECT count(*) filter (where id > 1), sum(age)  FROM MyTable");
    }

    @TestTemplate
    void testMultiFiltersMultipleAggregatesShouldPushdown() {
        util.verifyExecPlan(
                "SELECT count(*) filter (where id > 1), sum(age) filter (where id > 1) FROM MyTable");
    }
}
