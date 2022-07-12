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

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

import static org.apache.flink.table.api.DataTypes.STRING;

/** Test rule {@link RemoveUnreachableCoalesceArgumentsRule}. */
public class RemoveUnreachableCoalesceArgumentsRuleTest extends TableTestBase {

    private StreamTableTestUtil util;

    @Before
    public void before() {
        util = streamTestUtil(TableConfig.getDefault());

        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", STRING().nullable())
                                        .column("f1", STRING().notNull())
                                        .column("f2", STRING().nullable())
                                        .build())
                        .unboundedScanSource()
                        .build();

        util.tableEnv().createTable("T", sourceDescriptor);
    }

    @Test
    public void testOnlyLastNonNull() {
        util.verifyRelPlan("SELECT COALESCE(f0, f1) FROM T");
    }

    @Test
    public void testAllNullable() {
        util.verifyRelPlan("SELECT COALESCE(f0, f2) FROM T");
    }

    @Test
    public void testDropLastConstant() {
        util.verifyRelPlan("SELECT COALESCE(f0, f1, '-') FROM T");
    }

    @Test
    public void testDropCoalesce() {
        util.verifyRelPlan("SELECT COALESCE(f1, '-') FROM T");
    }

    @Test
    public void testFilterCoalesce() {
        util.verifyRelPlan("SELECT * FROM T WHERE COALESCE(f0, f1, '-') = 'abc'");
    }

    @Test
    public void testJoinCoalesce() {
        util.verifyRelPlan(
                "SELECT * FROM T t1 LEFT JOIN T t2 ON COALESCE(t1.f0, '-', t1.f2) = t2.f0");
    }

    @Test
    public void testMultipleCoalesces() {
        util.verifyRelPlan(
                "SELECT COALESCE(1),\n"
                        + "COALESCE(1, 2),\n"
                        + "COALESCE(cast(NULL as int), 2),\n"
                        + "COALESCE(1, cast(NULL as int)),\n"
                        + "COALESCE(cast(NULL as int), cast(NULL as int), 3),\n"
                        + "COALESCE(4, cast(NULL as int), cast(NULL as int), cast(NULL as int)),\n"
                        + "COALESCE('1'),\n"
                        + "COALESCE('1', '23'),\n"
                        + "COALESCE(cast(NULL as varchar), '2'),\n"
                        + "COALESCE('1', cast(NULL as varchar)),\n"
                        + "COALESCE(cast(NULL as varchar), cast(NULL as varchar), '3'),\n"
                        + "COALESCE('4', cast(NULL as varchar), cast(NULL as varchar), cast(NULL as varchar)),\n"
                        + "COALESCE(1.0),\n"
                        + "COALESCE(1.0, 2),\n"
                        + "COALESCE(cast(NULL as double), 2.0),\n"
                        + "COALESCE(cast(NULL as double), 2.0, 3.0),\n"
                        + "COALESCE(2.0, cast(NULL as double), 3.0),\n"
                        + "COALESCE(cast(NULL as double), cast(NULL as double))");
    }
}
