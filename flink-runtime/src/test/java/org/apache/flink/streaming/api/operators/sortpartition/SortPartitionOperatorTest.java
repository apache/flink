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
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link SortPartitionOperator}. */
class SortPartitionOperatorTest {

    @Test
    void testSortPartition() throws Exception {
        // 1.Test SortPartitionOperator sorting records by position field.
        SortPartitionOperator<Tuple2<Integer, String>> operator1 =
                createSortPartitionOperatorWithPositionField();
        OneInputStreamOperatorTestHarness<Tuple2<Integer, String>, Tuple2<Integer, String>>
                testHarness1 = new OneInputStreamOperatorTestHarness<>(operator1);
        Queue<Object> expectedOutput1 = new LinkedList<>();
        testHarness1.setup();
        testHarness1.processElement(new StreamRecord<>(Tuple2.of(3, "3")));
        testHarness1.processElement(new StreamRecord<>(Tuple2.of(1, "1")));
        testHarness1.endInput();
        testHarness1.close();
        expectedOutput1.add(new StreamRecord<>(Tuple2.of(1, "1")));
        expectedOutput1.add(new StreamRecord<>(Tuple2.of(3, "3")));
        TestHarnessUtil.assertOutputEquals(
                "The sort partition result is not correct.",
                expectedOutput1,
                testHarness1.getOutput());
        // 2.Test SortPartitionOperator sorting records by string field.
        SortPartitionOperator<TestPojo> operator2 = createSortPartitionOperatorWithStringField();
        OneInputStreamOperatorTestHarness<TestPojo, TestPojo> testHarness2 =
                new OneInputStreamOperatorTestHarness<>(operator2);
        Queue<Object> expectedOutput2 = new LinkedList<>();
        testHarness2.setup();
        testHarness2.processElement(new StreamRecord<>(new TestPojo("3", 3)));
        testHarness2.processElement(new StreamRecord<>(new TestPojo("1", 1)));
        testHarness2.endInput();
        testHarness2.close();
        expectedOutput2.add(new StreamRecord<>(new TestPojo("1", 1)));
        expectedOutput2.add(new StreamRecord<>(new TestPojo("3", 3)));
        TestHarnessUtil.assertOutputEquals(
                "The sort partition result is not correct.",
                expectedOutput2,
                testHarness2.getOutput());
        // 3.Test SortPartitionOperator sorting records by key selector.
        SortPartitionOperator<TestPojo> operator3 = createSortPartitionOperatorWithKeySelector();
        OneInputStreamOperatorTestHarness<TestPojo, TestPojo> testHarness3 =
                new OneInputStreamOperatorTestHarness<>(operator3);
        Queue<Object> expectedOutput3 = new LinkedList<>();
        testHarness3.setup();
        testHarness3.processElement(new StreamRecord<>(new TestPojo("3", 3)));
        testHarness3.processElement(new StreamRecord<>(new TestPojo("1", 1)));
        testHarness3.endInput();
        testHarness3.close();
        expectedOutput3.add(new StreamRecord<>(new TestPojo("1", 1)));
        expectedOutput3.add(new StreamRecord<>(new TestPojo("3", 3)));
        TestHarnessUtil.assertOutputEquals(
                "The sort partition result is not correct.",
                expectedOutput3,
                testHarness3.getOutput());
    }

    @Test
    void testOpenClose() throws Exception {
        SortPartitionOperator<Tuple2<Integer, String>> sortPartitionOperator =
                createSortPartitionOperatorWithPositionField();
        OneInputStreamOperatorTestHarness<Tuple2<Integer, String>, Tuple2<Integer, String>>
                testHarness = new OneInputStreamOperatorTestHarness<>(sortPartitionOperator);
        testHarness.open();
        testHarness.processElement(new StreamRecord<>(Tuple2.of(1, "1")));
        testHarness.endInput();
        testHarness.close();
        assertThat(testHarness.getOutput()).isNotEmpty();
    }

    private SortPartitionOperator<Tuple2<Integer, String>>
            createSortPartitionOperatorWithPositionField() {
        TypeInformation<Tuple2<Integer, String>> inputType =
                Types.TUPLE(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        int positionSortField = 0;
        Order sortOrder = Order.ASCENDING;
        return new SortPartitionOperator<>(inputType, positionSortField, sortOrder);
    }

    private SortPartitionOperator<TestPojo> createSortPartitionOperatorWithStringField() {
        TypeInformation<TestPojo> inputType = Types.POJO(TestPojo.class);
        String positionSortField = "value";
        Order sortOrder = Order.ASCENDING;
        return new SortPartitionOperator<>(inputType, positionSortField, sortOrder);
    }

    private SortPartitionOperator<TestPojo> createSortPartitionOperatorWithKeySelector() {
        TypeInformation<TestPojo> inputType = Types.POJO(TestPojo.class);
        Order sortOrder = Order.ASCENDING;
        return new SortPartitionOperator<>(inputType, TestPojo::getValue, sortOrder);
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
            if (object instanceof TestPojo) {
                TestPojo testPojo = (TestPojo) object;
                return testPojo.getKey().equals(getKey()) && testPojo.getValue().equals(getValue());
            }
            return false;
        }
    }
}
