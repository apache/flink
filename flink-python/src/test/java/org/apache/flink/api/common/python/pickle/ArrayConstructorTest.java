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

package org.apache.flink.api.common.python.pickle;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ArrayConstructor}. */
class ArrayConstructorTest {

    private final ArrayConstructor constructor = new ArrayConstructor();

    /**
     * Verifies that typecode 'l' (long) arrays are correctly constructed as long[], preserving
     * values that exceed Integer.MAX_VALUE. This was broken when args[0] == "l" was used (reference
     * equality) instead of "l".equals(args[0]) (value equality), causing the long[] branch to be
     * unreachable and silently truncating values to int.
     */
    @Test
    void testLongArrayPreservesValuesAboveIntMax() {
        ArrayList<Object> values = new ArrayList<>();
        values.add(3_000_000_000L); // exceeds Integer.MAX_VALUE (2_147_483_647)
        values.add(Long.MAX_VALUE);
        values.add(0L);
        values.add(-1L);

        // Simulate how pickle passes the typecode — as a deserialized String object,
        // not an interned literal. new String ensures it is a distinct object reference.
        Object[] args = new Object[] {new String("l"), values};

        Object result = constructor.construct(args);

        assertThat(result).isInstanceOf(long[].class);
        long[] longs = (long[]) result;
        assertThat(longs).hasSize(4);
        assertThat(longs[0]).isEqualTo(3_000_000_000L);
        assertThat(longs[1]).isEqualTo(Long.MAX_VALUE);
        assertThat(longs[2]).isEqualTo(0L);
        assertThat(longs[3]).isEqualTo(-1L);
    }

    @Test
    void testLongArrayWithSingleValue() {
        ArrayList<Object> values = new ArrayList<>();
        values.add(10_000_000_000L);

        Object[] args = new Object[] {new String("l"), values};

        Object result = constructor.construct(args);

        assertThat(result).isInstanceOf(long[].class);
        assertThat((long[]) result).containsExactly(10_000_000_000L);
    }

    @Test
    void testLongArrayWithEmptyList() {
        ArrayList<Object> values = new ArrayList<>();
        Object[] args = new Object[] {new String("l"), values};

        Object result = constructor.construct(args);

        assertThat(result).isInstanceOf(long[].class);
        assertThat((long[]) result).isEmpty();
    }

    @Test
    void testNonLongTypecodesDelegateToSuper() {
        // typecode 'i' (int) should fall through to the parent implementation
        ArrayList<Object> values = new ArrayList<>();
        values.add(42);

        Object[] args = new Object[] {new String("i"), values};

        Object result = constructor.construct(args);

        // parent returns int[] for typecode 'i'
        assertThat(result).isInstanceOf(int[].class);
        assertThat((int[]) result).containsExactly(42);
    }
}
