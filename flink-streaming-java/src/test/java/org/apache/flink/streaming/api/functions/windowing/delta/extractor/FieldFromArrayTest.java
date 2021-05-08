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

/** Tests for {@link FieldFromArray}. */
public class FieldFromArrayTest {

    String[] testStringArray = {"0", "1", "2", "3", "4"};
    Integer[] testIntegerArray = {10, 11, 12, 13, 14};
    int[] testIntArray = {20, 21, 22, 23, 24};

    @Test
    public void testStringArray() {
        for (int i = 0; i < this.testStringArray.length; i++) {
            assertEquals(
                    this.testStringArray[i],
                    new FieldFromArray<String>(i).extract(testStringArray));
        }
    }

    @Test
    public void testIntegerArray() {
        for (int i = 0; i < this.testIntegerArray.length; i++) {
            assertEquals(
                    this.testIntegerArray[i],
                    new FieldFromArray<String>(i).extract(testIntegerArray));
        }
    }

    @Test
    public void testIntArray() {
        for (int i = 0; i < this.testIntArray.length; i++) {
            assertEquals(
                    new Integer(this.testIntArray[i]),
                    new FieldFromArray<Integer>(i).extract(testIntArray));
        }
    }
}
