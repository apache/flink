/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.types.StringValue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link StringValueArray}. */
public class CharValueArrayTest {

    @Test
    public void testBoundedArray() {
        // one byte for length and one byte for character
        int count = StringValueArray.DEFAULT_CAPACITY_IN_BYTES / 2;

        ValueArray<StringValue> sva =
                new StringValueArray(StringValueArray.DEFAULT_CAPACITY_IN_BYTES);

        // fill the array
        for (int i = 0; i < count; i++) {
            assertFalse(sva.isFull());
            assertEquals(i, sva.size());

            assertTrue(sva.add(new StringValue(Character.toString((char) (i & 0x7F)))));

            assertEquals(i + 1, sva.size());
        }

        // array is now full
        assertTrue(sva.isFull());
        assertEquals(count, sva.size());

        // verify the array values
        int idx = 0;
        for (StringValue sv : sva) {
            assertEquals((idx++) & 0x7F, sv.getValue().charAt(0));
        }

        // add element past end of array
        assertFalse(sva.add(new StringValue(String.valueOf((char) count))));
        assertFalse(sva.addAll(sva));

        // test copy
        assertEquals(sva, sva.copy());

        // test copyTo
        StringValueArray svaTo = new StringValueArray();
        sva.copyTo(svaTo);
        assertEquals(sva, svaTo);

        // test clear
        sva.clear();
        assertEquals(0, sva.size());
    }

    @Test
    public void testBoundedArrayWithVariableLengthCharacters() {
        // characters alternatingly take 1 and 2 bytes (plus one byte for length)
        int count = 1280;

        ValueArray<StringValue> sva = new StringValueArray(3200);

        // fill the array
        for (int i = 0; i < count; i++) {
            assertFalse(sva.isFull());
            assertEquals(i, sva.size());

            assertTrue(sva.add(new StringValue(Character.toString((char) (i & 0xFF)))));

            assertEquals(i + 1, sva.size());
        }

        // array is now full
        assertTrue(sva.isFull());
        assertEquals(count, sva.size());

        // verify the array values
        int idx = 0;
        for (StringValue sv : sva) {
            assertEquals((idx++) & 0xFF, sv.getValue().charAt(0));
        }

        // add element past end of array
        assertFalse(sva.add(new StringValue(String.valueOf((char) count))));
        assertFalse(sva.addAll(sva));

        // test copy
        assertEquals(sva, sva.copy());

        // test copyTo
        StringValueArray svaTo = new StringValueArray();
        sva.copyTo(svaTo);
        assertEquals(sva, svaTo);

        // test clear
        sva.clear();
        assertEquals(0, sva.size());
    }

    @Test
    public void testUnboundedArray() {
        int count = 4096;

        ValueArray<StringValue> sva = new StringValueArray();

        // add several elements
        for (int i = 0; i < count; i++) {
            assertFalse(sva.isFull());
            assertEquals(i, sva.size());

            assertTrue(sva.add(new StringValue(String.valueOf((char) i))));

            assertEquals(i + 1, sva.size());
        }

        // array never fills
        assertFalse(sva.isFull());
        assertEquals(count, sva.size());

        // verify the array values
        int idx = 0;
        for (StringValue sv : sva) {
            assertEquals(idx++, sv.getValue().charAt(0));
        }

        // add element past end of array
        assertTrue(sva.add(new StringValue(String.valueOf((char) count))));
        assertTrue(sva.addAll(sva));

        // test copy
        assertEquals(sva, sva.copy());

        // test copyTo
        StringValueArray svaTo = new StringValueArray();
        sva.copyTo(svaTo);
        assertEquals(sva, svaTo);

        // test mark/reset
        int size = sva.size();
        sva.mark();
        assertTrue(sva.add(new StringValue()));
        assertEquals(size + 1, sva.size());
        sva.reset();
        assertEquals(size, sva.size());

        // test clear
        sva.clear();
        assertEquals(0, sva.size());
    }
}
