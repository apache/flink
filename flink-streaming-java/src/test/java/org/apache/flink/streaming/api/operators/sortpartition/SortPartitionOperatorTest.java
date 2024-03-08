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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for {@link SortPartitionOperator}. */
class SortPartitionOperatorTest {

    /** The test environment. */
    private Environment environment;

    /** The test stream task. */
    private StreamTask<?, ?> containingTask;

    /** The test stream config. */
    private StreamConfig config;

    @BeforeEach
    void before() throws Exception {
        environment = MockEnvironment.builder().build();
        containingTask =
                new StreamTask<Object, StreamOperator<Object>>(environment) {
                    @Override
                    protected void init() {}
                };
        config = new MockStreamConfig(new Configuration(), 1);
        config.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, 0.1);
    }

    @Test
    void testSetup() {
        // 1.Test SortPartitionOperator sorting record by position field.
        SortPartitionOperator<Tuple2<Integer, String>> operator1 =
                createSortPartitionOperatorWithPositionField();
        MockOutput<Tuple2<Integer, String>> tupleOutput = new MockOutput<>(new ArrayList<>());
        assertDoesNotThrow(() -> operator1.setup(containingTask, config, tupleOutput));
        // 2.Test SortPartitionOperator sorting record by string field.
        MockOutput<TestPojo> pojoOutput = new MockOutput<>(new ArrayList<>());
        SortPartitionOperator<TestPojo> operator2 = createSortPartitionOperatorWithStringField();
        assertDoesNotThrow(() -> operator2.setup(containingTask, config, pojoOutput));
        // 3.Test SortPartitionOperator sorting record by key selector.
        SortPartitionOperator<TestPojo> operator3 = createSortPartitionOperatorWithKeySelector();
        assertDoesNotThrow(() -> operator3.setup(containingTask, config, pojoOutput));
    }

    @Test
    void testProcessElement() throws Exception {
        // 1.Test SortPartitionOperator sorting record by position field.
        SortPartitionOperator<Tuple2<Integer, String>> operator1 =
                createSortPartitionOperatorWithPositionField();
        List<Tuple2<Integer, String>> tupleOutputList = new ArrayList<>();
        MockOutput<Tuple2<Integer, String>> tupleOutput = new MockOutput<>(tupleOutputList);
        operator1.setup(containingTask, config, tupleOutput);
        operator1.processElement(new StreamRecord<>(Tuple2.of(3, "3")));
        operator1.processElement(new StreamRecord<>(Tuple2.of(1, "1")));
        operator1.endInput();
        assertEquals(tupleOutputList.size(), 2);
        assertEquals(tupleOutputList.get(0), Tuple2.of(1, "1"));
        assertEquals(tupleOutputList.get(1), Tuple2.of(3, "3"));
        // 2.Test SortPartitionOperator sorting record by string field.
        SortPartitionOperator<TestPojo> operator2 = createSortPartitionOperatorWithStringField();
        List<TestPojo> pojoOutputList = new ArrayList<>();
        MockOutput<TestPojo> pojoOutput = new MockOutput<>(pojoOutputList);
        operator2.setup(containingTask, config, pojoOutput);
        operator2.processElement(new StreamRecord<>(new TestPojo("3", 3)));
        operator2.processElement(new StreamRecord<>(new TestPojo("1", 1)));
        operator2.endInput();
        assertEquals(pojoOutputList.size(), 2);
        assertEquals(pojoOutputList.get(0).getValue(), 1);
        assertEquals(pojoOutputList.get(1).getValue(), 3);
        // 3.Test SortPartitionOperator sorting record by key selector.
        SortPartitionOperator<TestPojo> operator3 = createSortPartitionOperatorWithKeySelector();
        List<TestPojo> pojoOutputListForKeySelector = new ArrayList<>();
        MockOutput<TestPojo> pojoOutputForKeySelector =
                new MockOutput<>(pojoOutputListForKeySelector);
        operator3.setup(containingTask, config, pojoOutputForKeySelector);
        operator3.processElement(new StreamRecord<>(new TestPojo("3", 3)));
        operator3.processElement(new StreamRecord<>(new TestPojo("1", 1)));
        operator3.endInput();
        assertEquals(pojoOutputListForKeySelector.size(), 2);
        assertEquals(pojoOutputListForKeySelector.get(0).getValue(), 1);
        assertEquals(pojoOutputListForKeySelector.get(1).getValue(), 3);
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
    }
}
