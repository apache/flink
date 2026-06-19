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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.operators.InputSelection.Builder;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link InputSelection}. */
class InputSelectionTest {
    @Test
    void testIsInputSelected() {
        assertThat(new Builder().build().isInputSelected(1)).isFalse();
        assertThat(new Builder().select(2).build().isInputSelected(1)).isFalse();

        assertThat(new Builder().select(1).build().isInputSelected(1)).isTrue();
        assertThat(new Builder().select(1).select(2).build().isInputSelected(1)).isTrue();
        assertThat(new Builder().select(-1).build().isInputSelected(1)).isTrue();

        assertThat(new Builder().select(64).build().isInputSelected(64)).isTrue();
    }

    @Test
    void testInputSelectionNormalization() {
        assertThat(InputSelection.ALL.areAllInputsSelected()).isTrue();

        assertThat(new Builder().select(1).select(2).build().areAllInputsSelected()).isFalse();
        assertThat(new Builder().select(1).select(2).build(2).areAllInputsSelected()).isTrue();

        assertThat(new Builder().select(1).select(2).select(3).build().areAllInputsSelected())
                .isFalse();
        assertThat(new Builder().select(1).select(2).select(3).build(3).areAllInputsSelected())
                .isTrue();

        assertThat(new Builder().select(1).select(3).build().areAllInputsSelected()).isFalse();
        assertThat(new Builder().select(1).select(3).build(3).areAllInputsSelected()).isFalse();

        assertThat(InputSelection.FIRST.areAllInputsSelected()).isFalse();
        assertThat(InputSelection.SECOND.areAllInputsSelected()).isFalse();
    }

    @Test
    void testInputSelectionNormalizationOverflow() {
        assertThatThrownBy(() -> new Builder().select(3).build(2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testFairSelectNextIndexOutOf2() {
        assertThat(InputSelection.ALL.fairSelectNextIndexOutOf2(3, 0)).isOne();
        assertThat(new Builder().select(1).select(2).build().fairSelectNextIndexOutOf2(3, 1))
                .isZero();

        assertThat(InputSelection.ALL.fairSelectNextIndexOutOf2(2, 0)).isOne();
        assertThat(InputSelection.ALL.fairSelectNextIndexOutOf2(2, 1)).isOne();
        assertThat(InputSelection.ALL.fairSelectNextIndexOutOf2(1, 0)).isZero();
        assertThat(InputSelection.ALL.fairSelectNextIndexOutOf2(1, 1)).isZero();
        assertThat(InputSelection.ALL.fairSelectNextIndexOutOf2(0, 0))
                .isEqualTo(InputSelection.NONE_AVAILABLE);
        assertThat(InputSelection.ALL.fairSelectNextIndexOutOf2(0, 1))
                .isEqualTo(InputSelection.NONE_AVAILABLE);

        assertThat(InputSelection.FIRST.fairSelectNextIndexOutOf2(1, 0)).isZero();
        assertThat(InputSelection.FIRST.fairSelectNextIndexOutOf2(3, 0)).isZero();
        assertThat(InputSelection.FIRST.fairSelectNextIndexOutOf2(2, 0))
                .isEqualTo(InputSelection.NONE_AVAILABLE);
        assertThat(InputSelection.FIRST.fairSelectNextIndexOutOf2(0, 0))
                .isEqualTo(InputSelection.NONE_AVAILABLE);

        assertThat(InputSelection.SECOND.fairSelectNextIndexOutOf2(2, 1)).isOne();
        assertThat(InputSelection.SECOND.fairSelectNextIndexOutOf2(3, 1)).isOne();
        assertThat(InputSelection.SECOND.fairSelectNextIndexOutOf2(1, 1))
                .isEqualTo(InputSelection.NONE_AVAILABLE);
        assertThat(InputSelection.SECOND.fairSelectNextIndexOutOf2(0, 1))
                .isEqualTo(InputSelection.NONE_AVAILABLE);
    }

    @Test
    void testFairSelectNextIndexWithAllInputsSelected() {
        assertThat(InputSelection.ALL.fairSelectNextIndex(7, 0)).isOne();
        assertThat(InputSelection.ALL.fairSelectNextIndex(7, 1)).isEqualTo(2);
        assertThat(InputSelection.ALL.fairSelectNextIndex(7, 2)).isZero();
        assertThat(InputSelection.ALL.fairSelectNextIndex(7, 0)).isOne();
        assertThat(InputSelection.ALL.fairSelectNextIndex(0, 2))
                .isEqualTo(InputSelection.NONE_AVAILABLE);

        assertThat(InputSelection.ALL.fairSelectNextIndex(-1, 10)).isEqualTo(11);
        assertThat(InputSelection.ALL.fairSelectNextIndex(-1, 63)).isZero();
        assertThat(InputSelection.ALL.fairSelectNextIndex(-1, 158)).isZero();
    }

    @Test
    void testFairSelectNextIndexWithSomeInputsSelected() {
        // combination of selection and availability is supposed to be 3, 5, 8:
        InputSelection selection =
                new Builder().select(2).select(3).select(4).select(5).select(8).build();
        int availableInputs =
                (int) new Builder().select(3).select(5).select(6).select(8).build().getInputMask();

        assertThat(selection.fairSelectNextIndex(availableInputs, 0)).isEqualTo(2);
        assertThat(selection.fairSelectNextIndex(availableInputs, 1)).isEqualTo(2);
        assertThat(selection.fairSelectNextIndex(availableInputs, 2)).isEqualTo(4);
        assertThat(selection.fairSelectNextIndex(availableInputs, 3)).isEqualTo(4);
        assertThat(selection.fairSelectNextIndex(availableInputs, 4)).isEqualTo(7);
        assertThat(selection.fairSelectNextIndex(availableInputs, 5)).isEqualTo(7);
        assertThat(selection.fairSelectNextIndex(availableInputs, 6)).isEqualTo(7);
        assertThat(selection.fairSelectNextIndex(availableInputs, 7)).isEqualTo(2);
        assertThat(selection.fairSelectNextIndex(availableInputs, 8)).isEqualTo(2);
        assertThat(selection.fairSelectNextIndex(availableInputs, 158)).isEqualTo(2);
        assertThat(selection.fairSelectNextIndex(0, 5)).isEqualTo(InputSelection.NONE_AVAILABLE);

        assertThat(new Builder().build().fairSelectNextIndex(-1, 5))
                .isEqualTo(InputSelection.NONE_AVAILABLE);
    }

    @Test
    void testUnsupportedFairSelectNextIndexOutOf2() {
        assertThatThrownBy(() -> InputSelection.ALL.fairSelectNextIndexOutOf2(7, 0))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** Tests for {@link Builder}. */
    static class BuilderTest {

        @Test
        void testSelect() {
            assertThat(new Builder().select(1).build().getInputMask()).isOne();
            assertThat(new Builder().select(1).select(2).select(3).build().getInputMask())
                    .isEqualTo(7L);

            assertThat(new Builder().select(64).build().getInputMask())
                    .isEqualTo(0x8000_0000_0000_0000L);
            assertThat(new Builder().select(-1).build().getInputMask())
                    .isEqualTo(0xffff_ffff_ffff_ffffL);
        }

        @Test
        void testIllegalInputId1() {
            assertThatThrownBy(() -> new Builder().select(-2))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void testIllegalInputId2() {
            assertThatThrownBy(() -> new Builder().select(65))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
