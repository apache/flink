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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link InputSelection}. */
public class InputSelectionTest {
    @Test
    public void testIsInputSelected() {
        assertFalse(new Builder().build().isInputSelected(1));
        assertFalse(new Builder().select(2).build().isInputSelected(1));

        assertTrue(new Builder().select(1).build().isInputSelected(1));
        assertTrue(new Builder().select(1).select(2).build().isInputSelected(1));
        assertTrue(new Builder().select(-1).build().isInputSelected(1));

        assertTrue(new Builder().select(64).build().isInputSelected(64));
    }

    @Test
    public void testInputSelectionNormalization() {
        assertTrue(InputSelection.ALL.areAllInputsSelected());

        assertFalse(new Builder().select(1).select(2).build().areAllInputsSelected());
        assertTrue(new Builder().select(1).select(2).build(2).areAllInputsSelected());

        assertFalse(new Builder().select(1).select(2).select(3).build().areAllInputsSelected());
        assertTrue(new Builder().select(1).select(2).select(3).build(3).areAllInputsSelected());

        assertFalse(new Builder().select(1).select(3).build().areAllInputsSelected());
        assertFalse(new Builder().select(1).select(3).build(3).areAllInputsSelected());

        assertFalse(InputSelection.FIRST.areAllInputsSelected());
        assertFalse(InputSelection.SECOND.areAllInputsSelected());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInputSelectionNormalizationOverflow() {
        new Builder().select(3).build(2);
    }

    @Test
    public void testFairSelectNextIndexOutOf2() {
        assertEquals(1, InputSelection.ALL.fairSelectNextIndexOutOf2(3, 0));
        assertEquals(0, new Builder().select(1).select(2).build().fairSelectNextIndexOutOf2(3, 1));

        assertEquals(1, InputSelection.ALL.fairSelectNextIndexOutOf2(2, 0));
        assertEquals(1, InputSelection.ALL.fairSelectNextIndexOutOf2(2, 1));
        assertEquals(0, InputSelection.ALL.fairSelectNextIndexOutOf2(1, 0));
        assertEquals(0, InputSelection.ALL.fairSelectNextIndexOutOf2(1, 1));
        assertEquals(
                InputSelection.NONE_AVAILABLE, InputSelection.ALL.fairSelectNextIndexOutOf2(0, 0));
        assertEquals(
                InputSelection.NONE_AVAILABLE, InputSelection.ALL.fairSelectNextIndexOutOf2(0, 1));

        assertEquals(0, InputSelection.FIRST.fairSelectNextIndexOutOf2(1, 0));
        assertEquals(0, InputSelection.FIRST.fairSelectNextIndexOutOf2(3, 0));
        assertEquals(
                InputSelection.NONE_AVAILABLE,
                InputSelection.FIRST.fairSelectNextIndexOutOf2(2, 0));
        assertEquals(
                InputSelection.NONE_AVAILABLE,
                InputSelection.FIRST.fairSelectNextIndexOutOf2(0, 0));

        assertEquals(1, InputSelection.SECOND.fairSelectNextIndexOutOf2(2, 1));
        assertEquals(1, InputSelection.SECOND.fairSelectNextIndexOutOf2(3, 1));
        assertEquals(
                InputSelection.NONE_AVAILABLE,
                InputSelection.SECOND.fairSelectNextIndexOutOf2(1, 1));
        assertEquals(
                InputSelection.NONE_AVAILABLE,
                InputSelection.SECOND.fairSelectNextIndexOutOf2(0, 1));
    }

    @Test
    public void testFairSelectNextIndexWithAllInputsSelected() {
        assertEquals(1, InputSelection.ALL.fairSelectNextIndex(7, 0));
        assertEquals(2, InputSelection.ALL.fairSelectNextIndex(7, 1));
        assertEquals(0, InputSelection.ALL.fairSelectNextIndex(7, 2));
        assertEquals(1, InputSelection.ALL.fairSelectNextIndex(7, 0));
        assertEquals(InputSelection.NONE_AVAILABLE, InputSelection.ALL.fairSelectNextIndex(0, 2));

        assertEquals(11, InputSelection.ALL.fairSelectNextIndex(-1, 10));
        assertEquals(0, InputSelection.ALL.fairSelectNextIndex(-1, 63));
        assertEquals(0, InputSelection.ALL.fairSelectNextIndex(-1, 158));
    }

    @Test
    public void testFairSelectNextIndexWithSomeInputsSelected() {
        // combination of selection and availability is supposed to be 3, 5, 8:
        InputSelection selection =
                new Builder().select(2).select(3).select(4).select(5).select(8).build();
        int availableInputs =
                (int) new Builder().select(3).select(5).select(6).select(8).build().getInputMask();

        assertEquals(2, selection.fairSelectNextIndex(availableInputs, 0));
        assertEquals(2, selection.fairSelectNextIndex(availableInputs, 1));
        assertEquals(4, selection.fairSelectNextIndex(availableInputs, 2));
        assertEquals(4, selection.fairSelectNextIndex(availableInputs, 3));
        assertEquals(7, selection.fairSelectNextIndex(availableInputs, 4));
        assertEquals(7, selection.fairSelectNextIndex(availableInputs, 5));
        assertEquals(7, selection.fairSelectNextIndex(availableInputs, 6));
        assertEquals(2, selection.fairSelectNextIndex(availableInputs, 7));
        assertEquals(2, selection.fairSelectNextIndex(availableInputs, 8));
        assertEquals(2, selection.fairSelectNextIndex(availableInputs, 158));
        assertEquals(InputSelection.NONE_AVAILABLE, selection.fairSelectNextIndex(0, 5));

        assertEquals(
                InputSelection.NONE_AVAILABLE, new Builder().build().fairSelectNextIndex(-1, 5));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedFairSelectNextIndexOutOf2() {
        InputSelection.ALL.fairSelectNextIndexOutOf2(7, 0);
    }

    /** Tests for {@link Builder}. */
    public static class BuilderTest {

        @Test
        public void testSelect() {
            assertEquals(1L, new Builder().select(1).build().getInputMask());
            assertEquals(7L, new Builder().select(1).select(2).select(3).build().getInputMask());

            assertEquals(0x8000_0000_0000_0000L, new Builder().select(64).build().getInputMask());
            assertEquals(0xffff_ffff_ffff_ffffL, new Builder().select(-1).build().getInputMask());
        }

        @Test(expected = IllegalArgumentException.class)
        public void testIllegalInputId1() {
            new Builder().select(-2);
        }

        @Test(expected = IllegalArgumentException.class)
        public void testIllegalInputId2() {
            new Builder().select(65);
        }
    }
}
