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

package org.apache.flink.table.runtime.operators.multipleinput.input;

import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.table.runtime.operators.multipleinput.MultipleInputTestBase;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link InputSelectionHandler}. */
public class InputSelectionHandlerTest extends MultipleInputTestBase {

    @Test
    public void testWithSamePriority() {
        List<InputSpec> inputSpecs =
                Arrays.asList(
                        new InputSpec(1, 0, createOneInputOperatorWrapper("input1"), 1),
                        new InputSpec(2, 0, createOneInputOperatorWrapper("input2"), 2),
                        new InputSpec(3, 0, createTwoInputOperatorWrapper("input3"), 1),
                        new InputSpec(4, 0, createTwoInputOperatorWrapper("input4"), 2),
                        new InputSpec(5, 0, createOneInputOperatorWrapper("input5"), 1));
        InputSelectionHandler handler = InputSelectionHandler.fromInputSpecs(inputSpecs);
        assertThat(handler.getInputSelection()).isEqualTo(InputSelection.ALL);

        List<Integer> inputIds = Arrays.asList(1, 2, 3, 4, 5);
        Collections.shuffle(inputIds);
        for (int inputId : inputIds) {
            handler.endInput(inputId);
            assertThat(handler.getInputSelection()).isEqualTo(InputSelection.ALL);
        }
    }

    @Test
    public void testWithDifferentPriority() {
        List<InputSpec> inputSpecs =
                Arrays.asList(
                        new InputSpec(1, 1, createOneInputOperatorWrapper("input1"), 1),
                        new InputSpec(2, 1, createOneInputOperatorWrapper("input2"), 2),
                        new InputSpec(3, 0, createTwoInputOperatorWrapper("input3"), 1),
                        new InputSpec(4, 0, createTwoInputOperatorWrapper("input4"), 2),
                        new InputSpec(5, 2, createOneInputOperatorWrapper("input5"), 1));
        InputSelectionHandler handler = InputSelectionHandler.fromInputSpecs(inputSpecs);
        assertThat(handler.getInputSelection())
                .isEqualTo(new InputSelection.Builder().select(3).select(4).build(5));

        handler.endInput(3);
        assertThat(handler.getInputSelection())
                .isEqualTo(new InputSelection.Builder().select(3).select(4).build(5));

        handler.endInput(4);
        assertThat(handler.getInputSelection())
                .isEqualTo(new InputSelection.Builder().select(1).select(2).build(5));

        handler.endInput(2);
        assertThat(handler.getInputSelection())
                .isEqualTo(new InputSelection.Builder().select(1).select(2).build(5));

        handler.endInput(1);
        assertThat(handler.getInputSelection())
                .isEqualTo(new InputSelection.Builder().select(5).build(5));

        handler.endInput(5);
        assertThat(handler.getInputSelection()).isEqualTo(InputSelection.ALL);
    }
}
