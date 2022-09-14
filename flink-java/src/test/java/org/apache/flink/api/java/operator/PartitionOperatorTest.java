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
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for partitioning. */
class PartitionOperatorTest {

    /** Custom data type, for testing purposes. */
    public static class CustomPojo implements Serializable, Comparable<CustomPojo> {
        private Integer number;
        private String name;

        public CustomPojo() {}

        public CustomPojo(Integer number, String name) {
            this.number = number;
            this.name = name;
        }

        public Integer getNumber() {
            return number;
        }

        public void setNumber(Integer number) {
            this.number = number;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public int compareTo(CustomPojo o) {
            return Integer.compare(this.number, o.number);
        }
    }

    /** Custom data type with nested type, for testing purposes. */
    public static class NestedPojo implements Serializable {
        private CustomPojo nested;
        private Long outer;

        public NestedPojo() {}

        public NestedPojo(CustomPojo nested, Long outer) {
            this.nested = nested;
            this.outer = outer;
        }

        public CustomPojo getNested() {
            return nested;
        }

        public void setNested(CustomPojo nested) {
            this.nested = nested;
        }

        public Long getOuter() {
            return outer;
        }

        public void setOuter(Long outer) {
            this.outer = outer;
        }
    }

    private DataSet<Tuple2<Integer, String>> getTupleDataSet(ExecutionEnvironment env) {
        return env.fromElements(
                new Tuple2<>(1, "first"),
                new Tuple2<>(2, "second"),
                new Tuple2<>(3, "third"),
                new Tuple2<>(4, "fourth"),
                new Tuple2<>(5, "fifth"),
                new Tuple2<>(6, "sixth"));
    }

    private DataSet<CustomPojo> getPojoDataSet(ExecutionEnvironment env) {
        return env.fromElements(
                new CustomPojo(1, "first"),
                new CustomPojo(2, "second"),
                new CustomPojo(3, "third"),
                new CustomPojo(4, "fourth"),
                new CustomPojo(5, "fifth"),
                new CustomPojo(6, "sixth"));
    }

    private DataSet<NestedPojo> getNestedPojoDataSet(ExecutionEnvironment env) {
        return env.fromElements(
                new NestedPojo(new CustomPojo(1, "first"), 1L),
                new NestedPojo(new CustomPojo(2, "second"), 2L),
                new NestedPojo(new CustomPojo(3, "third"), 3L),
                new NestedPojo(new CustomPojo(4, "fourth"), 4L),
                new NestedPojo(new CustomPojo(5, "fifth"), 5L),
                new NestedPojo(new CustomPojo(6, "sixth"), 6L));
    }

    @Test
    void testRebalance() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<Tuple2<Integer, String>> ds = getTupleDataSet(env);
        ds.rebalance();
    }

    @Test
    void testHashPartitionByField1() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<Tuple2<Integer, String>> ds = getTupleDataSet(env);
        ds.partitionByHash(0);
    }

    @Test
    void testHashPartitionByField2() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<Tuple2<Integer, String>> ds = getTupleDataSet(env);
        ds.partitionByHash(0, 1);
    }

    @Test
    void testHashPartitionByFieldOutOfRange() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<Tuple2<Integer, String>> ds = getTupleDataSet(env);
        assertThatThrownBy(() -> ds.partitionByHash(0, 1, 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testHashPartitionByFieldName1() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<CustomPojo> ds = getPojoDataSet(env);
        ds.partitionByHash("number");
    }

    @Test
    void testHashPartitionByFieldName2() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<CustomPojo> ds = getPojoDataSet(env);
        ds.partitionByHash("number", "name");
    }

    @Test
    void testHashPartitionByInvalidFieldName() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<CustomPojo> ds = getPojoDataSet(env);
        assertThatThrownBy(() -> ds.partitionByHash("number", "name", "invalidField"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRangePartitionByFieldName1() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<CustomPojo> ds = getPojoDataSet(env);
        ds.partitionByRange("number");
    }

    @Test
    void testRangePartitionByFieldName2() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<CustomPojo> ds = getPojoDataSet(env);
        ds.partitionByRange("number", "name");
    }

    @Test
    void testRangePartitionByInvalidFieldName() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<CustomPojo> ds = getPojoDataSet(env);
        assertThatThrownBy(() -> ds.partitionByRange("number", "name", "invalidField"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRangePartitionByField1() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<Tuple2<Integer, String>> ds = getTupleDataSet(env);
        ds.partitionByRange(0);
    }

    @Test
    void testRangePartitionByField2() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<Tuple2<Integer, String>> ds = getTupleDataSet(env);
        ds.partitionByRange(0, 1);
    }

    @Test
    void testRangePartitionWithEmptyIndicesKey() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSource<Tuple2<Tuple2<Integer, Integer>, Integer>> ds =
                env.fromElements(
                        new Tuple2<>(new Tuple2<>(1, 1), 1),
                        new Tuple2<>(new Tuple2<>(2, 2), 2),
                        new Tuple2<>(new Tuple2<>(2, 2), 2));
        assertThatThrownBy(() -> ds.partitionByRange(new int[] {}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRangePartitionByFieldOutOfRange() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<Tuple2<Integer, String>> ds = getTupleDataSet(env);
        assertThatThrownBy(() -> ds.partitionByRange(0, 1, 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testHashPartitionWithOrders() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<Tuple2<Integer, String>> ds = getTupleDataSet(env);
        assertThatThrownBy(() -> ds.partitionByHash(1).withOrders(Order.ASCENDING))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testRebalanceWithOrders() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<Tuple2<Integer, String>> ds = getTupleDataSet(env);
        assertThatThrownBy(() -> ds.rebalance().withOrders(Order.ASCENDING))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testRangePartitionWithOrders() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<Tuple2<Integer, String>> ds = getTupleDataSet(env);
        ds.partitionByRange(0).withOrders(Order.ASCENDING);
    }

    @Test
    void testRangePartitionWithTooManyOrders() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<Tuple2<Integer, String>> ds = getTupleDataSet(env);
        assertThatThrownBy(
                        () -> ds.partitionByRange(0).withOrders(Order.ASCENDING, Order.DESCENDING))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRangePartitionByComplexKeyWithOrders() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSource<Tuple2<Tuple2<Integer, Integer>, Integer>> ds =
                env.fromElements(
                        new Tuple2<>(new Tuple2<>(1, 1), 1),
                        new Tuple2<>(new Tuple2<>(2, 2), 2),
                        new Tuple2<>(new Tuple2<>(2, 2), 2));
        ds.partitionByRange(0, 1).withOrders(Order.ASCENDING, Order.DESCENDING);
    }

    @Test
    void testRangePartitionByComplexKeyWithTooManyOrders() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSource<Tuple2<Tuple2<Integer, Integer>, Integer>> ds =
                env.fromElements(
                        new Tuple2<>(new Tuple2<>(1, 1), 1),
                        new Tuple2<>(new Tuple2<>(2, 2), 2),
                        new Tuple2<>(new Tuple2<>(2, 2), 2));
        assertThatThrownBy(
                        () -> ds.partitionByRange(0).withOrders(Order.ASCENDING, Order.DESCENDING))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRangePartitionBySelectorComplexKeyWithOrders() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<NestedPojo> ds = getNestedPojoDataSet(env);
        ds.partitionByRange((KeySelector<NestedPojo, CustomPojo>) NestedPojo::getNested)
                .withOrders(Order.ASCENDING);
    }

    @Test
    void testRangePartitionBySelectorComplexKeyWithTooManyOrders() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<NestedPojo> ds = getNestedPojoDataSet(env);
        assertThatThrownBy(
                        () ->
                                ds.partitionByRange(
                                                (KeySelector<NestedPojo, CustomPojo>)
                                                        value -> value.getNested())
                                        .withOrders(Order.ASCENDING, Order.DESCENDING))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRangePartitionCustomPartitionerByFieldId() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<Tuple2<Integer, String>> ds = getTupleDataSet(env);
        ds.partitionCustom((Partitioner<Integer>) (key, numPartitions) -> 1, 0);
    }

    @Test
    void testRangePartitionInvalidCustomPartitionerByFieldId() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<Tuple2<Integer, String>> ds = getTupleDataSet(env);
        assertThatThrownBy(
                        () ->
                                ds.partitionCustom(
                                        (Partitioner<Integer>) (key, numPartitions) -> 1, 1))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testRangePartitionCustomPartitionerByFieldName() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<CustomPojo> ds = getPojoDataSet(env);
        ds.partitionCustom((Partitioner<Integer>) (key, numPartitions) -> 1, "number");
    }

    @Test
    void testRangePartitionInvalidCustomPartitionerByFieldName() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<CustomPojo> ds = getPojoDataSet(env);
        assertThatThrownBy(
                        () ->
                                ds.partitionCustom(
                                        (Partitioner<Integer>) (key, numPartitions) -> 1, "name"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testRangePartitionCustomPartitionerByKeySelector() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<CustomPojo> ds = getPojoDataSet(env);
        ds.partitionCustom(
                (Partitioner<Integer>) (key, numPartitions) -> 1,
                (KeySelector<CustomPojo, Integer>) CustomPojo::getNumber);
    }
}
