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

import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.utils.PlannerMocks;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CorrelVariableNormalizerShuttle}. */
class CorrelVariableNormalizerShuttleTest {

    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    @BeforeEach
    void before() {
        FlinkRelBuilder relBuilder = PlannerMocks.create().getPlannerContext().createRelBuilder();
        rexBuilder = relBuilder.getRexBuilder();
        cluster = relBuilder.getCluster();
    }

    @Test
    void testRemapsVariablesSetForFilterProjectAndJoin() {
        CorrelationId oldId = new CorrelationId(5);
        CorrelationId newId = new CorrelationId(1);
        RelNode input = oneRow();

        RelNode filter =
                LogicalFilter.create(
                        input,
                        rexBuilder.makeLiteral(true),
                        com.google.common.collect.ImmutableSet.of(oldId));
        assertThat(normalize(filter).getVariablesSet()).containsExactly(newId);

        RelNode project =
                LogicalProject.create(
                        input,
                        List.of(),
                        List.of(rexBuilder.makeInputRef(input, 0)),
                        List.of("ZERO"),
                        Set.of(oldId));
        assertThat(normalize(project).getVariablesSet()).containsExactly(newId);

        RelNode join =
                LogicalJoin.create(
                        oneRow(),
                        oneRow(),
                        List.of(),
                        rexBuilder.makeLiteral(true),
                        Set.of(oldId),
                        JoinRelType.INNER);
        assertThat(normalize(join).getVariablesSet()).containsExactly(newId);
    }

    private RelNode normalize(RelNode relNode) {
        return relNode.accept(new CorrelVariableNormalizerShuttle(rexBuilder));
    }

    private RelNode oneRow() {
        return LogicalValues.createOneRow(cluster);
    }
}
