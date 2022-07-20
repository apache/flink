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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.multipleinput.TableOperatorWrapper.Edge;

import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableOperatorWrapper}. */
public class TableOperatorWrapperTest extends MultipleInputTestBase {

    @Test
    public void testBasicInfo() {
        TestingOneInputStreamOperator inOperator1 = new TestingOneInputStreamOperator();
        TestingOneInputStreamOperator inOperator2 = new TestingOneInputStreamOperator();
        TestingTwoInputStreamOperator outOperator = new TestingTwoInputStreamOperator();
        TableOperatorWrapper<TestingOneInputStreamOperator> wrapper1 =
                createOneInputOperatorWrapper(inOperator1, "test1");

        TableOperatorWrapper<TestingOneInputStreamOperator> wrapper2 =
                createOneInputOperatorWrapper(inOperator2, "test2");

        TableOperatorWrapper<TestingTwoInputStreamOperator> wrapper3 =
                createTwoInputOperatorWrapper(outOperator, "test3");
        wrapper3.addInput(wrapper1, 1);
        wrapper3.addInput(wrapper2, 2);

        assertThat(wrapper1.getInputEdges()).isEmpty();
        assertThat(wrapper1.getInputWrappers()).isEmpty();
        assertThat(wrapper1.getOutputWrappers()).containsExactly(wrapper3);
        assertThat(wrapper1.getOutputEdges()).containsExactly(new Edge(wrapper1, wrapper3, 1));

        assertThat(wrapper2.getInputEdges()).isEmpty();
        assertThat(wrapper2.getInputWrappers()).isEmpty();
        assertThat(wrapper2.getOutputWrappers()).containsExactly(wrapper3);
        assertThat(wrapper2.getOutputEdges()).containsExactly(new Edge(wrapper2, wrapper3, 2));

        assertThat(wrapper3.getOutputEdges()).isEmpty();
        assertThat(wrapper3.getOutputWrappers()).isEmpty();
        assertThat(wrapper3.getInputWrappers()).isEqualTo(Arrays.asList(wrapper1, wrapper2));
        assertThat(wrapper3.getInputEdges())
                .isEqualTo(
                        Arrays.asList(
                                new Edge(wrapper1, wrapper3, 1), new Edge(wrapper2, wrapper3, 2)));
    }

    @Test
    public void testCreateOperator() throws Exception {
        TestingOneInputStreamOperator operator = new TestingOneInputStreamOperator();
        TableOperatorWrapper<TestingOneInputStreamOperator> wrapper =
                createOneInputOperatorWrapper(operator, "test");
        StreamOperatorParameters<RowData> parameters = createStreamOperatorParameters();
        wrapper.createOperator(parameters);

        assertThat(wrapper.getStreamOperator()).isEqualTo(operator);

        // create operator again, will throw exception
        assertThatThrownBy(() -> wrapper.createOperator(parameters))
                .hasMessageContaining("This operator has been initialized");
    }

    @Test
    public void testEndInput() throws Exception {
        StreamOperatorParameters<RowData> parameters = createStreamOperatorParameters();
        TestingOneInputStreamOperator inOperator1 = new TestingOneInputStreamOperator();
        TestingOneInputStreamOperator inOperator2 = new TestingOneInputStreamOperator();
        TestingTwoInputStreamOperator outOperator = new TestingTwoInputStreamOperator();
        TableOperatorWrapper<TestingOneInputStreamOperator> wrapper1 =
                createOneInputOperatorWrapper(inOperator1, "test1");
        wrapper1.createOperator(parameters);

        TableOperatorWrapper<TestingOneInputStreamOperator> wrapper2 =
                createOneInputOperatorWrapper(inOperator2, "test2");
        wrapper2.createOperator(parameters);

        TableOperatorWrapper<TestingTwoInputStreamOperator> wrapper3 =
                createTwoInputOperatorWrapper(outOperator, "test3");
        wrapper3.addInput(wrapper1, 1);
        wrapper3.addInput(wrapper2, 2);
        wrapper3.createOperator(parameters);

        // initialized status
        assertThat(inOperator1.isEnd()).isFalse();
        assertThat(inOperator2.isEnd()).isFalse();
        assertThat(outOperator.getEndInputs()).isEmpty();

        // end first input
        wrapper1.endOperatorInput(1);
        assertThat(inOperator1.isEnd()).isTrue();
        assertThat(wrapper1.getEndedInputCount()).isEqualTo(1);
        assertThat(inOperator2.isEnd()).isFalse();
        assertThat(wrapper2.getEndedInputCount()).isEqualTo(0);
        assertThat(outOperator.getEndInputs()).containsExactly(1);
        assertThat(wrapper3.getEndedInputCount()).isEqualTo(1);

        // end second input
        wrapper2.endOperatorInput(1);
        assertThat(inOperator1.isEnd()).isTrue();
        assertThat(wrapper1.getEndedInputCount()).isEqualTo(1);
        assertThat(inOperator2.isEnd()).isTrue();
        assertThat(wrapper2.getEndedInputCount()).isEqualTo(1);
        assertThat(outOperator.getEndInputs()).isEqualTo(Arrays.asList(1, 2));
        assertThat(wrapper3.getEndedInputCount()).isEqualTo(2);
    }

    @Test
    public void testClose() throws Exception {
        TestingOneInputStreamOperator operator = new TestingOneInputStreamOperator();
        TableOperatorWrapper<TestingOneInputStreamOperator> wrapper =
                createOneInputOperatorWrapper(operator, "test");
        StreamOperatorParameters<RowData> parameters = createStreamOperatorParameters();
        wrapper.createOperator(parameters);
        assertThat(wrapper.getStreamOperator()).isEqualTo(operator);

        assertThat(operator.isClosed()).isFalse();
        assertThat(wrapper.isClosed()).isFalse();
        wrapper.close();
        assertThat(wrapper.isClosed()).isTrue();
        assertThat(operator.isClosed()).isTrue();

        // close again
        wrapper.close();
        assertThat(wrapper.isClosed()).isTrue();
        assertThat(operator.isClosed()).isTrue();
    }
}
