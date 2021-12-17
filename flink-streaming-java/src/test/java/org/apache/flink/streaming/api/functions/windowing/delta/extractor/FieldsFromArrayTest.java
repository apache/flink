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

package org.apache.flink.streaming.api.functions.windowing.delta.extractor;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link FieldsFromArray}. */
public class FieldsFromArrayTest {

    String[] testStringArray = {"0", "1", "2", "3", "4"};
    Integer[] testIntegerArray = {10, 11, 12, 13, 14};
    int[] testIntArray = {20, 21, 22, 23, 24};

    @Test
    public void testStringArray() {
        // check single field extraction
        for (int i = 0; i < testStringArray.length; i++) {
            String[] tmp = {testStringArray[i]};
            arrayEqualityCheck(
                    tmp, new FieldsFromArray<String>(String.class, i).extract(testStringArray));
        }

        // check reverse order
        String[] reverseOrder = new String[testStringArray.length];
        for (int i = 0; i < testStringArray.length; i++) {
            reverseOrder[i] = testStringArray[testStringArray.length - i - 1];
        }
        arrayEqualityCheck(
                reverseOrder,
                new FieldsFromArray<String>(String.class, 4, 3, 2, 1, 0).extract(testStringArray));

        // check picking fields and reorder
        String[] crazyOrder = {testStringArray[4], testStringArray[1], testStringArray[2]};
        arrayEqualityCheck(
                crazyOrder,
                new FieldsFromArray<String>(String.class, 4, 1, 2).extract(testStringArray));
    }

    @Test
    public void testIntegerArray() {
        // check single field extraction
        for (int i = 0; i < testIntegerArray.length; i++) {
            Integer[] tmp = {testIntegerArray[i]};
            arrayEqualityCheck(
                    tmp, new FieldsFromArray<Integer>(Integer.class, i).extract(testIntegerArray));
        }

        // check reverse order
        Integer[] reverseOrder = new Integer[testIntegerArray.length];
        for (int i = 0; i < testIntegerArray.length; i++) {
            reverseOrder[i] = testIntegerArray[testIntegerArray.length - i - 1];
        }
        arrayEqualityCheck(
                reverseOrder,
                new FieldsFromArray<Integer>(Integer.class, 4, 3, 2, 1, 0)
                        .extract(testIntegerArray));

        // check picking fields and reorder
        Integer[] crazyOrder = {testIntegerArray[4], testIntegerArray[1], testIntegerArray[2]};
        arrayEqualityCheck(
                crazyOrder,
                new FieldsFromArray<Integer>(Integer.class, 4, 1, 2).extract(testIntegerArray));
    }

    @Test
    public void testIntArray() {
        for (int i = 0; i < testIntArray.length; i++) {
            Integer[] tmp = {testIntArray[i]};
            arrayEqualityCheck(
                    tmp, new FieldsFromArray<Integer>(Integer.class, i).extract(testIntArray));
        }

        // check reverse order
        Integer[] reverseOrder = new Integer[testIntArray.length];
        for (int i = 0; i < testIntArray.length; i++) {
            reverseOrder[i] = testIntArray[testIntArray.length - i - 1];
        }
        arrayEqualityCheck(
                reverseOrder,
                new FieldsFromArray<Integer>(Integer.class, 4, 3, 2, 1, 0).extract(testIntArray));

        // check picking fields and reorder
        Integer[] crazyOrder = {testIntArray[4], testIntArray[1], testIntArray[2]};
        arrayEqualityCheck(
                crazyOrder,
                new FieldsFromArray<Integer>(Integer.class, 4, 1, 2).extract(testIntArray));
    }

    private void arrayEqualityCheck(Object[] array1, Object[] array2) {
        assertEquals("The result arrays must have the same length", array1.length, array2.length);
        for (int i = 0; i < array1.length; i++) {
            assertEquals("Unequal fields at position " + i, array1[i], array2[i]);
        }
    }
}
