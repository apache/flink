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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;

/** Test for {@link FlinkRexExtract}. */
public class FlinkRexExtractTest {
    RexBuilder rexBuilder;
    FlinkRexExtract rexExtract;
    Predicate<RexNode> onlyA;
    RexNode a0, a1, a2, a3;
    RexNode b0, b1, b2;

    @Before
    public void setUp() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
        rexBuilder = new RexBuilder(typeFactory);
        rexExtract = new FlinkRexExtract(rexBuilder);
        a0 = atom(0);
        a1 = atom(1);
        a2 = atom(2);
        a3 = atom(3);
        b0 = atom(4);
        b1 = atom(5);
        b2 = atom(6);
        onlyA = rex -> Arrays.asList(a0, a1, a2, a3).contains(rex);
    }

    @Test
    public void testSimple() {
        // ((a0 and b0) or a1) => (a0 or a1)
        RexNode rex = or(and(a0, b0), a1);
        RexNode expected = or(a0, a1);
        assertEquals(expected, rexExtract.extract(rex, onlyA));
    }

    @Test
    public void testDeepNested() {
        // ((((((a0 and b0) or a1) and b1) or a2) and b2) or a3) => (a0 or a1 or a2 or a3)
        RexNode rex = or(and(or(and(or(and(a0, b0), a1), b1), a2), b2), a3);
        RexNode expected = or(a0, a1, a2, a3);
        assertEquals(expected, rexExtract.extract(rex, onlyA));
    }

    RexNode atom(int ordinal) {
        RelDataType type = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
        RexNode zeroLiteral = rexBuilder.makeLiteral(0, type, false);
        RexInputRef inputRef = rexBuilder.makeInputRef(type, ordinal);
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, inputRef, zeroLiteral);
    }

    RexNode or(RexNode... rexs) {
        return RexUtil.composeDisjunction(rexBuilder, Arrays.asList(rexs));
    }

    RexNode and(RexNode... rexs) {
        return RexUtil.composeConjunction(rexBuilder, Arrays.asList(rexs));
    }
}
