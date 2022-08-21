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

package org.apache.flink.api.java.operator;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DataSet#first(int)}. */
class FirstNOperatorTest {

    // TUPLE DATA

    private final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData =
            new ArrayList<>();

    private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo =
            new TupleTypeInfo<>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    @Test
    void testUngroupedFirstN() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        tupleDs.first(1);

        // should work
        tupleDs.first(10);

        // should not work n == 0
        assertThatThrownBy(() -> tupleDs.first(0)).isInstanceOf(InvalidProgramException.class);

        // should not work n == -1
        assertThatThrownBy(() -> tupleDs.first(-1)).isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testGroupedFirstN() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        tupleDs.groupBy(2).first(1);

        // should work
        tupleDs.groupBy(1, 3).first(10);

        // should not work n == 0
        assertThatThrownBy(() -> tupleDs.groupBy(0).first(0))
                .isInstanceOf(InvalidProgramException.class);

        // should not work n == -1
        assertThatThrownBy(() -> tupleDs.groupBy(2).first(-1))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testGroupedSortedFirstN() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        tupleDs.groupBy(2).sortGroup(4, Order.ASCENDING).first(1);

        // should work
        tupleDs.groupBy(1, 3).sortGroup(4, Order.ASCENDING).first(10);

        // should not work n == 0
        assertThatThrownBy(() -> tupleDs.groupBy(0).sortGroup(4, Order.ASCENDING).first(0))
                .isInstanceOf(InvalidProgramException.class);

        // should not work n == -1
        assertThatThrownBy(() -> tupleDs.groupBy(2).sortGroup(4, Order.ASCENDING).first(-1))
                .isInstanceOf(InvalidProgramException.class);
    }
}
