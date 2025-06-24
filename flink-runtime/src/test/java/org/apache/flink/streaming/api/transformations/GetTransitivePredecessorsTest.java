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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@code getTransitivePredecessors} method of {@link Transformation}. */
class GetTransitivePredecessorsTest {

    private TestTransformation<Integer> commonNode;
    private Transformation<Integer> midNode;

    @BeforeEach
    void setup() {
        commonNode = new TestTransformation<>("commonNode", new MockIntegerTypeInfo(), 1);
        midNode =
                new OneInputTransformation<>(
                        commonNode,
                        "midNode",
                        new DummyOneInputOperator(),
                        new MockIntegerTypeInfo(),
                        1);
    }

    @Test
    void testTwoInputTransformation() {
        Transformation<Integer> topNode =
                new TwoInputTransformation<>(
                        commonNode,
                        midNode,
                        "topNode",
                        new DummyTwoInputOperator<>(),
                        midNode.getOutputType(),
                        1);
        List<Transformation<?>> predecessors = topNode.getTransitivePredecessors();
        assertThat(predecessors.size()).isEqualTo(3);
        assertThat(commonNode.getNumGetTransitivePredecessor()).isEqualTo(1);
    }

    @Test
    void testUnionTransformation() {
        Transformation<Integer> topNode =
                new UnionTransformation<>(Arrays.asList(commonNode, midNode));
        List<Transformation<?>> predecessors = topNode.getTransitivePredecessors();
        assertThat(predecessors.size()).isEqualTo(3);
        assertThat(commonNode.getNumGetTransitivePredecessor()).isEqualTo(1);
    }

    @Test
    void testBroadcastStateTransformation() {
        Transformation<Integer> topNode =
                new AbstractBroadcastStateTransformation<>(
                        "topNode", commonNode, midNode, null, midNode.getOutputType(), 1);
        List<Transformation<?>> predecessors = topNode.getTransitivePredecessors();
        assertThat(predecessors.size()).isEqualTo(3);
        assertThat(commonNode.getNumGetTransitivePredecessor()).isEqualTo(0);
    }

    @Test
    void testAbstractMultipleInputTransformation() {
        Transformation<Integer> topNode =
                new AbstractMultipleInputTransformation<Integer>(
                        "topNode",
                        SimpleOperatorFactory.of(new DummyTwoInputOperator<>()),
                        midNode.getOutputType(),
                        1) {
                    @Override
                    public List<Transformation<?>> getInputs() {
                        return Arrays.asList(commonNode, midNode);
                    }
                };
        List<Transformation<?>> predecessors = topNode.getTransitivePredecessors();
        assertThat(predecessors.size()).isEqualTo(3);
        assertThat(commonNode.getNumGetTransitivePredecessor()).isEqualTo(1);
    }

    /** A test implementation of {@link Transformation}. */
    private static class TestTransformation<T> extends Transformation<T> {
        private int numGetTransitivePredecessor = 0;

        public TestTransformation(String name, TypeInformation<T> outputType, int parallelism) {
            super(name, outputType, parallelism);
        }

        @Override
        protected List<Transformation<?>> getTransitivePredecessorsInternal() {
            ++numGetTransitivePredecessor;
            return Collections.singletonList(this);
        }

        @Override
        public List<Transformation<?>> getInputs() {
            return Collections.emptyList();
        }

        public int getNumGetTransitivePredecessor() {
            return numGetTransitivePredecessor;
        }
    }

    /** A test implementation of {@link OneInputTransformation}. */
    private static class DummyOneInputOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {}
    }

    /** A test implementation of {@link TwoInputStreamOperator}. */
    private static class DummyTwoInputOperator<T> extends AbstractStreamOperator<T>
            implements TwoInputStreamOperator<T, T, T> {

        @Override
        public void processElement1(StreamRecord<T> element) throws Exception {
            output.collect(element);
        }

        @Override
        public void processElement2(StreamRecord<T> element) throws Exception {
            output.collect(element);
        }
    }

    /** A test implementation of {@link TypeInformation}. */
    private static class MockIntegerTypeInfo extends GenericTypeInfo<Integer> {
        public MockIntegerTypeInfo() {
            super(Integer.class);
        }
    }
}
