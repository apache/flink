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

package org.apache.flink.streaming.api.operators.sortpartition;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link KeyedSortPartitionOperator}. */
class KeyedSortPartitionOperatorTest {

    @Test
    void testSortPartition() throws Exception {
        // 1.Test KeyedSortPartitionOperator sorting records by position field.
        KeyedSortPartitionOperator<Tuple2<Integer, String>, Integer> operator1 =
                createSortPartitionOperatorWithPositionField();
        KeyedOneInputStreamOperatorTestHarness<
                        Integer, Tuple2<Integer, String>, Tuple2<Integer, String>>
                testHarness1 =
                        new KeyedOneInputStreamOperatorTestHarness<>(
                                operator1, ignored -> 1, Types.INT);
        Queue<Object> expectedOutput1 = new LinkedList<>();
        long earlierTimestamp = 1L;
        long laterTimestamp = 3L;
        testHarness1.open();
        testHarness1.processElement(new StreamRecord<>(Tuple2.of(3, "3"), earlierTimestamp));
        testHarness1.processElement(new StreamRecord<>(Tuple2.of(1, "1"), laterTimestamp));
        testHarness1.endInput();
        testHarness1.close();
        expectedOutput1.add(new StreamRecord<>(Tuple2.of(1, "1"), laterTimestamp));
        expectedOutput1.add(new StreamRecord<>(Tuple2.of(3, "3"), earlierTimestamp));
        TestHarnessUtil.assertOutputEquals(
                "The sort partition result is not correct.",
                expectedOutput1,
                testHarness1.getOutput());
        // 2.Test KeyedSortPartitionOperator sorting records by string field.
        KeyedSortPartitionOperator<TestPojo, String> operator2 =
                createSortPartitionOperatorWithStringField();
        KeyedOneInputStreamOperatorTestHarness<String, TestPojo, TestPojo> testHarness2 =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator2, ignored -> "group", Types.STRING);
        Queue<Object> expectedOutput2 = new LinkedList<>();
        testHarness2.open();
        testHarness2.processElement(new StreamRecord<>(new TestPojo("3", 3), earlierTimestamp));
        testHarness2.processElement(new StreamRecord<>(new TestPojo("1", 1), laterTimestamp));
        testHarness2.endInput();
        testHarness2.close();
        expectedOutput2.add(
                new StreamRecord<>(new SortPartitionOperatorTest.TestPojo("1", 1), laterTimestamp));
        expectedOutput2.add(
                new StreamRecord<>(
                        new SortPartitionOperatorTest.TestPojo("3", 3), earlierTimestamp));
        TestHarnessUtil.assertOutputEquals(
                "The sort partition result is not correct.",
                expectedOutput2,
                testHarness2.getOutput());
        // 3.Test KeyedSortPartitionOperator sorting records by key selector.
        KeyedSortPartitionOperator<TestPojo, String> operator3 =
                createSortPartitionOperatorWithKeySelector();
        KeyedOneInputStreamOperatorTestHarness<String, TestPojo, TestPojo> testHarness3 =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator3, ignored -> "group", Types.STRING);
        Queue<Object> expectedOutput3 = new LinkedList<>();
        testHarness3.open();
        testHarness3.processElement(new StreamRecord<>(new TestPojo("3", 3), earlierTimestamp));
        testHarness3.processElement(new StreamRecord<>(new TestPojo("1", 1), laterTimestamp));
        testHarness3.endInput();
        testHarness3.close();
        expectedOutput3.add(
                new StreamRecord<>(new SortPartitionOperatorTest.TestPojo("1", 1), laterTimestamp));
        expectedOutput3.add(
                new StreamRecord<>(
                        new SortPartitionOperatorTest.TestPojo("3", 3), earlierTimestamp));
        TestHarnessUtil.assertOutputEquals(
                "The sort partition result is not correct.",
                expectedOutput3,
                testHarness3.getOutput());
    }

    @Test
    void testOpenClose() throws Exception {
        KeyedSortPartitionOperator<Tuple2<Integer, String>, Integer> sortPartitionOperator =
                createSortPartitionOperatorWithPositionField();
        KeyedOneInputStreamOperatorTestHarness<
                        Integer, Tuple2<Integer, String>, Tuple2<Integer, String>>
                testHarness =
                        new KeyedOneInputStreamOperatorTestHarness<>(
                                sortPartitionOperator, ignored -> 1, Types.INT);
        testHarness.open();
        testHarness.processElement(new StreamRecord<>(Tuple2.of(1, "1")));
        testHarness.endInput();
        testHarness.close();
        assertThat(testHarness.getOutput()).isNotEmpty();
        assertThat(((StreamRecord<?>) testHarness.getOutput().poll()).hasTimestamp()).isFalse();
    }

    private KeyedSortPartitionOperator<Tuple2<Integer, String>, Integer>
            createSortPartitionOperatorWithPositionField() {
        TypeInformation<Tuple2<Integer, String>> inputType =
                Types.TUPLE(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        int positionSortField = 0;
        Order sortOrder = Order.ASCENDING;
        return new KeyedSortPartitionOperator<>(inputType, positionSortField, sortOrder);
    }

    private KeyedSortPartitionOperator<TestPojo, String>
            createSortPartitionOperatorWithStringField() {
        TypeInformation<TestPojo> inputType = Types.POJO(TestPojo.class);
        String positionSortField = "value";
        Order sortOrder = Order.ASCENDING;
        return new KeyedSortPartitionOperator<>(inputType, positionSortField, sortOrder);
    }

    private KeyedSortPartitionOperator<TestPojo, String>
            createSortPartitionOperatorWithKeySelector() {
        TypeInformation<TestPojo> inputType = Types.POJO(TestPojo.class);
        Order sortOrder = Order.ASCENDING;
        return new KeyedSortPartitionOperator<>(inputType, TestPojo::getValue, sortOrder);
    }

    /** The test pojo. */
    public static class TestPojo implements Serializable {

        public String key;

        public Integer value;

        public TestPojo() {}

        public TestPojo(String key, Integer value) {
            this.key = key;
            this.value = value;
        }

        public Integer getValue() {
            return value;
        }

        public void setValue(Integer value) {
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        @Override
        public boolean equals(Object object) {
            if (object instanceof SortPartitionOperatorTest.TestPojo) {
                SortPartitionOperatorTest.TestPojo testPojo =
                        (SortPartitionOperatorTest.TestPojo) object;
                return testPojo.getKey().equals(getKey()) && testPojo.getValue().equals(getValue());
            }
            return false;
        }
    }
}
