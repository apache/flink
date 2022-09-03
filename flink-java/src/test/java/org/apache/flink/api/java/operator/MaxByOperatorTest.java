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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DataSet#maxBy(int...)}. */
class MaxByOperatorTest {

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

    /** This test validates that no exceptions is thrown when an empty dataset calls maxBy(). */
    @Test
    void testMaxByKeyFieldsDataset() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        tupleDs.maxBy(4, 0, 1, 2, 3);
    }

    private final List<CustomType> customTypeData = new ArrayList<>();

    /**
     * This test validates that an InvalidProgramException is thrown when maxBy is used on a custom
     * data type.
     */
    @Test
    void testCustomKeyFieldsDataset() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        this.customTypeData.add(new CustomType());

        DataSet<CustomType> customDs = env.fromCollection(customTypeData);
        // should not work: groups on custom type
        assertThatThrownBy(() -> customDs.maxBy(0)).isInstanceOf(InvalidProgramException.class);
    }

    /**
     * This test validates that an index which is out of bounds throws an IndexOutOfBoundsException.
     */
    @Test
    void testOutOfTupleBoundsDataset1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, key out of tuple bounds
        assertThatThrownBy(() -> tupleDs.maxBy(5)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    /**
     * This test validates that an index which is out of bounds throws an IndexOutOfBoundsException.
     */
    @Test
    void testOutOfTupleBoundsDataset2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, key out of tuple bounds
        assertThatThrownBy(() -> tupleDs.maxBy(-1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    /**
     * This test validates that an index which is out of bounds throws an IndexOutOfBoundsException.
     */
    @Test
    void testOutOfTupleBoundsDataset3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, key out of tuple bounds
        assertThatThrownBy(() -> tupleDs.maxBy(1, 2, 3, 4, -1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    // ---------------------------- GROUPING TESTS BELOW --------------------------------------

    /** This test validates that no exceptions is thrown when an empty grouping calls maxBy(). */
    @Test
    void testMaxByKeyFieldsGrouping() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        UnsortedGrouping<Tuple5<Integer, Long, String, Long, Integer>> groupDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo).groupBy(0);

        // should work
        groupDs.maxBy(4, 0, 1, 2, 3);
    }

    /**
     * This test validates that an InvalidProgramException is thrown when maxBy is used on a custom
     * data type.
     */
    @Test
    void testCustomKeyFieldsGrouping() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        this.customTypeData.add(new CustomType());

        // should not work: groups on custom type
        assertThatThrownBy(
                        () -> {
                            UnsortedGrouping<CustomType> groupDs =
                                    env.fromCollection(customTypeData).groupBy(0);
                            groupDs.maxBy(0);
                        })
                .isInstanceOf(InvalidProgramException.class);
    }

    /**
     * This test validates that an index which is out of bounds throws an IndexOutOfBoundsException.
     */
    @Test
    void testOutOfTupleBoundsGrouping1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        UnsortedGrouping<Tuple5<Integer, Long, String, Long, Integer>> groupDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo).groupBy(0);

        // should not work, key out of tuple bounds
        assertThatThrownBy(() -> groupDs.maxBy(5)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    /**
     * This test validates that an index which is out of bounds throws an IndexOutOfBoundsException.
     */
    @Test
    void testOutOfTupleBoundsGrouping2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        UnsortedGrouping<Tuple5<Integer, Long, String, Long, Integer>> groupDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo).groupBy(0);

        // should not work, key out of tuple bounds
        assertThatThrownBy(() -> groupDs.maxBy(-1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    /**
     * This test validates that an index which is out of bounds throws an IndexOutOfBoundsException.
     */
    @Test
    void testOutOfTupleBoundsGrouping3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        UnsortedGrouping<Tuple5<Integer, Long, String, Long, Integer>> groupDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo).groupBy(0);

        // should not work, key out of tuple bounds
        assertThatThrownBy(() -> groupDs.maxBy(1, 2, 3, 4, -1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    /** Validates that no ClassCastException happens should not fail e.g. like in FLINK-8255. */
    @Test
    void testMaxByRowTypeInfoKeyFieldsDataset() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        TypeInformation[] types = new TypeInformation[] {Types.INT, Types.INT};

        String[] fieldNames = new String[] {"id", "value"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);
        DataSet tupleDs = env.fromCollection(Collections.singleton(new Row(2)), rowTypeInfo);

        assertThatThrownBy(() -> tupleDs.maxBy(0)).isInstanceOf(InvalidProgramException.class);
    }

    /** Validates that no ClassCastException happens should not fail e.g. like in FLINK-8255. */
    @Test
    void testMaxByRowTypeInfoKeyFieldsForUnsortedGrouping() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation<?>[] types = new TypeInformation[] {Types.INT, Types.INT};

        String[] fieldNames = new String[] {"id", "value"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);

        UnsortedGrouping<?> groupDs =
                env.fromCollection(Collections.singleton(new Row(2)), rowTypeInfo).groupBy(0);

        assertThatThrownBy(() -> groupDs.maxBy(1)).isInstanceOf(InvalidProgramException.class);
    }

    /** Custom data type, for testing purposes. */
    public static class CustomType implements Serializable {

        private static final long serialVersionUID = 1L;

        public int myInt;
        public long myLong;
        public String myString;

        public CustomType() {}

        public CustomType(int i, long l, String s) {
            myInt = i;
            myLong = l;
            myString = s;
        }

        @Override
        public String toString() {
            return myInt + "," + myLong + "," + myString;
        }
    }
}
