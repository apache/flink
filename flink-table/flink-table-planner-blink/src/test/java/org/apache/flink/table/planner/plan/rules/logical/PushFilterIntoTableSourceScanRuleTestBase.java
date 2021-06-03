/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License");; you may not use this file except in compliance
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

import org.apache.flink.table.planner.expressions.utils.Func1$;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Test;

/** Test base for testing rule which pushes filter into table source. */
public abstract class PushFilterIntoTableSourceScanRuleTestBase extends TableTestBase {

    protected TableTestUtil util;

    @Test
    public void testCanPushDown() {
        util.verifyRelPlan("SELECT * FROM MyTable WHERE amount > 2");
    }

    @Test
    public void testCanPushDownWithVirtualColumn() {
        util.verifyRelPlan("SELECT * FROM VirtualTable WHERE amount > 2");
    }

    @Test
    public void testCannotPushDown() {
        // TestFilterableTableSource only accept predicates with `amount`
        util.verifyRelPlan("SELECT * FROM MyTable WHERE price > 10");
    }

    @Test
    public void testCannotPushDownWithVirtualColumn() {
        // TestFilterableTableSource only accept predicates with `amount`
        util.verifyRelPlan("SELECT * FROM VirtualTable WHERE price > 10");
    }

    @Test
    public void testPartialPushDown() {
        util.verifyRelPlan("SELECT * FROM MyTable WHERE amount > 2 AND price > 10");
    }

    @Test
    public void testPartialPushDownWithVirtualColumn() {
        util.verifyRelPlan("SELECT * FROM VirtualTable WHERE amount > 2 AND price > 10");
    }

    @Test
    public void testFullyPushDown() {
        util.verifyRelPlan("SELECT * FROM MyTable WHERE amount > 2 AND amount < 10");
    }

    @Test
    public void testFullyPushDownWithVirtualColumn() {
        util.verifyRelPlan("SELECT * FROM VirtualTable WHERE amount > 2 AND amount < 10");
    }

    @Test
    public void testPartialPushDown2() {
        util.verifyRelPlan("SELECT * FROM MyTable WHERE amount > 2 OR price > 10");
    }

    @Test
    public void testPartialPushDown2WithVirtualColumn() {
        util.verifyRelPlan("SELECT * FROM VirtualTable WHERE amount > 2 OR price > 10");
    }

    @Test
    public void testCannotPushDown3() {
        util.verifyRelPlan("SELECT * FROM MyTable WHERE amount > 2 OR amount < 10");
    }

    @Test
    public void testCannotPushDown3WithVirtualColumn() {
        util.verifyRelPlan("SELECT * FROM VirtualTable WHERE amount > 2 OR amount < 10");
    }

    @Test
    public void testUnconvertedExpression() {
        util.verifyRelPlan(
                "SELECT * FROM MyTable WHERE\n"
                        + "    amount > 2 AND id < 100 AND CAST(amount AS BIGINT) > 10");
    }

    @Test
    public void testWithUdf() {
        util.addFunction("myUdf", Func1$.MODULE$);
        util.verifyRelPlan("SELECT * FROM MyTable WHERE amount > 2 AND myUdf(amount) < 32");
    }

    @Test
    public void testLowerUpperPushdown() {
        util.verifyRelPlan("SELECT * FROM MTable WHERE LOWER(a) = 'foo' AND UPPER(b) = 'bar'");
    }

    @Test
    public void testWithInterval() {
        util.verifyRelPlan(
                "SELECT * FROM MTable\n"
                        + "WHERE TIMESTAMPADD(HOUR, 5, a) >= b\n"
                        + "OR\n"
                        + "TIMESTAMPADD(YEAR, 2, b) >= a");
    }

    @Test
    public void testCannotPushDownIn() {
        // this test is to avoid filter push down rules throwing exceptions
        // when dealing with IN expressions, this is because Filter in calcite
        // requires its condition to be "flat"
        util.verifyRelPlan("SELECT * FROM MyTable WHERE name IN ('Alice', 'Bob', 'Dave')");
    }

    @Test
    public void testWithNullLiteral() {
        util.verifyRelPlan(
                "WITH MyView AS (SELECT CASE\n"
                        + "  WHEN amount > 0 THEN name\n"
                        + "  ELSE CAST(NULL AS STRING)\n"
                        + "  END AS a\n"
                        + "  FROM MyTable)\n"
                        + "SELECT a FROM MyView WHERE a IS NOT NULL\n");
    }
}
