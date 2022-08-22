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

package org.apache.flink.table.planner.alias;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.hint.JoinStrategy;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.junit.Test;

/** Tests clearing join hint with invalid propagation in batch. */
public class ClearJoinHintWithInvalidPropagationShuttleTest
        extends ClearJoinHintWithInvalidPropagationShuttleTestBase {
    @Override
    TableTestUtil getTableTestUtil() {
        return batchTestUtil(TableConfig.getDefault());
    }

    @Override
    boolean isBatchMode() {
        return true;
    }

    @Test
    public void testNoNeedToClearJoinHint() {
        // SELECT /*+ BROADCAST(t1)*/t1.a FROM t1 JOIN t2 ON t1.a = t2.a
        RelHint joinHintInView =
                RelHint.builder(JoinStrategy.BROADCAST.getJoinHintName()).hintOption("t1").build();

        RelNode root =
                builder.scan("t1")
                        .scan("t2")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(joinHintInView)
                        .build();
        verifyRelPlan(root);
    }

    @Test
    public void testClearJoinHintWithInvalidPropagationToViewWhileViewHasJoinHints() {
        //  SELECT /*+ BROADCAST(t3)*/t4.a FROM (
        //      SELECT /*+ BROADCAST(t1)*/t1.a FROM t1 JOIN t2 ON t1.a = t2.a
        //  ) t4 JOIN t3 ON t4.a = t3.a
        RelHint joinHintInView =
                RelHint.builder(JoinStrategy.BROADCAST.getJoinHintName()).hintOption("t1").build();

        RelHint joinHintRoot =
                RelHint.builder(JoinStrategy.BROADCAST.getJoinHintName()).hintOption("t3").build();

        RelHint aliasHint = RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("t4").build();

        RelNode root =
                builder.scan("t1")
                        .scan("t2")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(joinHintInView, aliasHint)
                        .scan("t3")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(joinHintRoot)
                        .build();
        verifyRelPlan(root);
    }

    @Test
    public void testClearJoinHintWithInvalidPropagationToViewWhileViewHasNoJoinHints() {
        //  SELECT /*+ BROADCAST(t3)*/t4.a FROM (
        //      SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a
        //  ) t4 JOIN t3 ON t4.a = t3.a
        RelHint joinHintRoot =
                RelHint.builder(JoinStrategy.BROADCAST.getJoinHintName()).hintOption("t3").build();

        RelHint aliasHint = RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("t4").build();

        RelNode root =
                builder.scan("t1")
                        .scan("t2")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(aliasHint)
                        .scan("t3")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(joinHintRoot)
                        .build();
        verifyRelPlan(root);
    }

    @Test
    public void testClearJoinHintWithoutPropagatingToView() {
        //  SELECT /*+ BROADCAST(t1)*/t4.a FROM (
        //      SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a
        //  ) t4 JOIN t3 ON t4.a = t3.a
        RelHint joinHintRoot =
                RelHint.builder(JoinStrategy.BROADCAST.getJoinHintName()).hintOption("t1").build();

        RelHint aliasHint = RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("t4").build();

        RelNode root =
                builder.scan("t1")
                        .scan("t2")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(aliasHint)
                        .scan("t3")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(joinHintRoot)
                        .build();
        verifyRelPlan(root);
    }
}
