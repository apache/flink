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

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.internal.StatementSetImpl;
import org.apache.flink.table.planner.hint.JoinStrategy;
import org.apache.flink.table.planner.plan.hints.batch.JoinHintTestBase;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.calcite.rel.RelNode;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** A test class for {@link ClearQueryBlockAliasResolver}. */
public class ClearQueryBlockAliasResolverTest extends JoinHintTestBase {

    // use any join hint for test
    @Override
    protected String getTestSingleJoinHint() {
        return JoinStrategy.BROADCAST.getJoinHintName();
    }

    @Override
    protected String getDisabledOperatorName() {
        return "HashJoin";
    }

    /**
     * Customize logic to verify the RelNode tree by sql.
     *
     * <p>Currently, mainly copy from {@link TableTestBase} and customize it.
     */
    @Override
    protected void verifyRelPlanByCustom(String sql) {
        Table table = util.tableEnv().sqlQuery(sql);
        RelNode relNode = TableTestUtil.toRelNode(table);
        verifyRelPlanAfterResolverWithSql(sql, Collections.singletonList(relNode));
    }

    /**
     * Customize logic to verify the RelNode tree by StatementSet.
     *
     * <p>Currently, mainly copy from {@link TableTestBase} and customize it.
     */
    @Override
    protected void verifyRelPlanByCustom(StatementSet set) {
        StatementSetImpl<?> testStmtSet = (StatementSetImpl<?>) set;

        List<RelNode> relNodes =
                testStmtSet.getOperations().stream()
                        .map(node -> util.getPlanner().translateToRel(node))
                        .collect(Collectors.toList());
        verifyRelPlanAfterResolverWithStatementSet(relNodes);
    }

    /**
     * Customize logic to verify the RelNode tree.
     *
     * <p>Currently, mainly copy from {@link TableTestBase} and customize it.
     */
    private void verifyRelPlanAfterResolverWithSql(String sql, List<RelNode> relNodes) {
        relNodes = clearQueryBlockAlias(relNodes);
        String astPlan = buildAstPlanWithQueryBlockAlias(relNodes);

        util.assertEqualsOrExpand("sql", sql, true);
        util.assertEqualsOrExpand("ast", astPlan, false);
    }

    private void verifyRelPlanAfterResolverWithStatementSet(List<RelNode> relNodes) {
        relNodes = clearQueryBlockAlias(relNodes);
        String astPlan = buildAstPlanWithQueryBlockAlias(relNodes);

        util.assertEqualsOrExpand("ast", astPlan, false);
    }

    private List<RelNode> clearQueryBlockAlias(List<RelNode> relNodes) {
        JoinHintResolver joinHintResolver = new JoinHintResolver();
        relNodes = joinHintResolver.resolve(relNodes);
        ClearQueryBlockAliasResolver clearQueryBlockAliasResolver =
                new ClearQueryBlockAliasResolver();
        return clearQueryBlockAliasResolver.resolve(relNodes);
    }
}
