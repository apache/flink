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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Plan tests for the FROM_CHANGELOG built-in process table function. Uses {@link
 * ExplainDetail#CHANGELOG_MODE} to verify changelog mode propagation through the plan.
 */
public class FromChangelogTest extends TableTestBase {

    private static final List<ExplainDetail> CHANGELOG_MODE =
            Collections.singletonList(ExplainDetail.CHANGELOG_MODE);

    private TableTestUtil util;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());
    }

    @Test
    void testRetract() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE cdc_stream ("
                                + "  id INT,"
                                + "  op STRING,"
                                + "  name STRING"
                                + ") WITH ('connector' = 'values')");
        util.verifyRelPlan(
                "SELECT * FROM FROM_CHANGELOG(input => TABLE cdc_stream)", CHANGELOG_MODE);
    }

    @Test
    void testRetractPartitionBy() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE cdc_stream ("
                                + "  id INT,"
                                + "  op STRING,"
                                + "  name STRING"
                                + ") WITH ('connector' = 'values')");
        util.verifyRelPlan(
                "SELECT * FROM FROM_CHANGELOG(input => TABLE cdc_stream PARTITION BY id)",
                CHANGELOG_MODE);
    }
}
