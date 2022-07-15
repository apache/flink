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

package org.apache.flink.table.planner.plan.common;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/** Test for {@link JoinPushTransitivePredicatesRule}. */
@RunWith(Parameterized.class)
@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
public class JoinPushTransitivePredicatesRuleTest extends TableTestBase {

    enum Mode {
        STREAM,
        BATCH
    }

    @Parameterized.Parameter public Mode mode;

    @Parameterized.Parameters(name = "mode = {0}")
    public static Collection<Mode> parameters() {
        return Arrays.asList(Mode.values());
    }

    TableTestUtil util;

    @Before
    public void setUp() {
        TableConfig conf = TableConfig.getDefault();
        util = mode == Mode.STREAM ? streamTestUtil(conf) : batchTestUtil(conf);
        // language=SQL
        executeSql("create table A(a0 int) with ('connector' = 'values', 'bounded' = 'true')");
        // language=SQL
        executeSql("create table B(b0 int) with ('connector' = 'values', 'bounded' = 'true')");
    }

    @After
    public void clean() {
        // language=SQL
        executeSql("drop table A");
        // language=SQL
        executeSql("drop table B");
    }

    @Test
    public void testMoreThen() {
        // language=SQL
        util.verifyRelPlan("select * from A join B on a0 = b0 and a0 > 0");
    }

    @Test
    public void testNotEquals() {
        // language=SQL
        util.verifyRelPlan("select * from A join B on a0 = b0 and a0 <> 0");
    }

    private void executeSql(String sql) {
        util.tableEnv().executeSql(sql);
    }
}
