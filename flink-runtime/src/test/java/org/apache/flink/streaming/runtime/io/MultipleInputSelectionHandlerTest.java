/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.api.operators.InputSelection;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MultipleInputSelectionHandler}. */
class MultipleInputSelectionHandlerTest {

    @Test
    void testShouldSetAvailableForAnotherInput() {
        InputSelection secondAndThird = new InputSelection.Builder().select(2).select(3).build();

        MultipleInputSelectionHandler selectionHandler =
                new MultipleInputSelectionHandler(() -> secondAndThird, 3);
        selectionHandler.nextSelection();

        assertThat(selectionHandler.shouldSetAvailableForAnotherInput()).isFalse();

        selectionHandler.setUnavailableInput(0);
        assertThat(selectionHandler.shouldSetAvailableForAnotherInput()).isFalse();

        selectionHandler.setUnavailableInput(2);
        assertThat(selectionHandler.shouldSetAvailableForAnotherInput()).isTrue();

        selectionHandler.setAvailableInput(0);
        assertThat(selectionHandler.shouldSetAvailableForAnotherInput()).isTrue();

        selectionHandler.setAvailableInput(2);
        assertThat(selectionHandler.shouldSetAvailableForAnotherInput()).isFalse();
    }

    @Test
    void testLargeInputCount() {
        int inputCount = MultipleInputSelectionHandler.MAX_SUPPORTED_INPUT_COUNT;

        InputSelection.Builder builder = new InputSelection.Builder();
        for (int i = 1; i <= inputCount; i++) {
            builder.select(i);
        }
        InputSelection allSelected = builder.build();

        MultipleInputSelectionHandler selectionHandler =
                new MultipleInputSelectionHandler(() -> allSelected, inputCount);
        selectionHandler.nextSelection();

        for (int i = 0; i < inputCount - 1; i++) {
            selectionHandler.setUnavailableInput(i);
        }
        assertThat(selectionHandler.isAnyInputAvailable()).isTrue();
        selectionHandler.setUnavailableInput(inputCount - 1);
        assertThat(selectionHandler.isAnyInputAvailable()).isFalse();
    }
}
