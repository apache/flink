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

package org.apache.flink.table.planner.hint;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Tests clearing state ttl hint with invalid propagation in stream. */
public class ClearStateTtlHintsWithInvalidPropagationShuttleTest
        extends ClearQueryHintsWithInvalidPropagationShuttleTestBase {

    @Override
    TableTestUtil getTableTestUtil() {
        return streamTestUtil(TableConfig.getDefault());
    }

    @Override
    boolean isBatchMode() {
        return false;
    }

    @BeforeEach
    void before() throws Exception {
        super.before();
        enableCapitalize();
    }

    @Test
    void testNoNeedToClearStateTtlHintOnJoin() {
        //  SELECT t4.a FROM (
        //      SELECT /*+ StaTe_TtL("t1" = "1d", "t2" = "7d")*/t1.a FROM t1 JOIN t2 ON t1.a = t2.a
        //  ) t4 JOIN t3 ON t4.a = t3.a
        Map<String, String> hintOptions = new HashMap<>();
        hintOptions.put("t1", "1d");
        hintOptions.put("t2", "7d");

        RelHint stateTtlHint = RelHint.builder("StaTe_TtL").hintOptions(hintOptions).build();

        RelHint aliasHint = RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("t4").build();

        RelNode root =
                builder.scan("t1")
                        .scan("t2")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(stateTtlHint, aliasHint)
                        .scan("t3")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .build();

        verifyRelPlan(root);
    }

    @Test
    void testClearStateTtlHintOnJoin() {
        //  SELECT /*+ StaTe_TtL("t4" = "9d", "t3" = "12d")*/t4.a FROM (
        //      SELECT /*+ StaTe_TtL("t1" = "1d", "t2" = "7d")*/t1.a FROM t1 JOIN t2 ON t1.a = t2.a
        //  ) t4 JOIN t3 ON t4.a = t3.a
        Map<String, String> hintOptionsInner = new HashMap<>();
        hintOptionsInner.put("t1", "1d");
        hintOptionsInner.put("t2", "7d");

        RelHint stateTtlHintInner =
                RelHint.builder("StaTe_TtL").hintOptions(hintOptionsInner).build();

        Map<String, String> hintOptionsOuter = new HashMap<>();
        hintOptionsOuter.put("t4", "9d");
        hintOptionsOuter.put("t3", "12d");
        RelHint stateTtlHintOuter =
                RelHint.builder("StAte_tTl").hintOptions(hintOptionsOuter).build();

        RelHint aliasHint = RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("t4").build();

        RelNode root =
                builder.scan("t1")
                        .scan("t2")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(stateTtlHintInner, aliasHint)
                        .scan("t3")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(stateTtlHintOuter)
                        .build();

        verifyRelPlan(root);
    }

    @Test
    void testNoNeedToClearStateTtlHintOnAggregate() {
        //  SELECT tmp.a, max(tmp.cnt) as max FROM (
        //      SELECT /*+ StaTe_TtL("t4" = "1d")*/t4.a, t4.b, count(t4.c) as cnt
        //          FROM t4 GROUP BY t4.a, t4.b
        //  ) tmp GROUP BY tmp.a
        Map<String, String> hintOptions = new HashMap<>();
        hintOptions.put("t4", "1d");

        RelHint stateTtlHint = RelHint.builder("StaTe_TtL").hintOptions(hintOptions).build();

        RelHint aliasHint = RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("tmp").build();
        RelNode root =
                builder.scan("t4")
                        .aggregate(
                                builder.groupKey(ImmutableBitSet.of(0, 1)),
                                Collections.singletonList(
                                        builder.count(builder.field(1, 0, "c")).as("cnt")))
                        .hints(stateTtlHint, aliasHint)
                        .project(builder.field(1, 0, "a"), builder.field(1, 0, "cnt"))
                        .aggregate(
                                builder.groupKey(ImmutableBitSet.of(0)),
                                Collections.singletonList(
                                        builder.max(builder.field(1, 0, "cnt")).as("max")))
                        .project(builder.field(1, 0, "a"), builder.field(1, 0, "max"))
                        .build();

        verifyRelPlan(root);
    }

    @Test
    void testClearStateTtlHintOnAggregate() {
        //  SELECT /*+ StaTe_TtL("tmp" = "2d")*/tmp.a, max(tmp.cnt) as max FROM (
        //      SELECT /*+ StaTe_TtL("t4" = "1d")*/t4.a, t4.b, count(t4.c) as cnt
        //          FROM t4 GROUP BY t4.a, t4.b
        //  ) tmp GROUP BY tmp.a
        Map<String, String> hintOptionsInner = new HashMap<>();
        hintOptionsInner.put("t4", "1d");

        RelHint stateTtlHintInner =
                RelHint.builder("StaTe_TtL").hintOptions(hintOptionsInner).build();

        Map<String, String> hintOptionsOuter = new HashMap<>();
        hintOptionsOuter.put("tmp", "2d");

        RelHint stateTtlHintOuter =
                RelHint.builder("StaTe_TtL").hintOptions(hintOptionsOuter).build();

        RelHint aliasHint = RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("tmp").build();
        RelNode root =
                builder.scan("t4")
                        .aggregate(
                                builder.groupKey(ImmutableBitSet.of(0, 1)),
                                Collections.singletonList(
                                        builder.count(builder.field(1, 0, "c")).as("cnt")))
                        .hints(stateTtlHintInner, aliasHint)
                        .project(builder.field(1, 0, "a"), builder.field(1, 0, "cnt"))
                        .aggregate(
                                builder.groupKey(ImmutableBitSet.of(0)),
                                Collections.singletonList(
                                        builder.max(builder.field(1, 0, "cnt")).as("max")))
                        .project(builder.field(1, 0, "a"), builder.field(1, 0, "max"))
                        .hints(stateTtlHintOuter)
                        .build();

        verifyRelPlan(root);
    }
}
