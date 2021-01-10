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

package org.apache.flink.api.common.accumulators;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LongMinimumTest {

    @Test
    public void testGet() {
        LongMinimum min = new LongMinimum();
        assertEquals(Long.MAX_VALUE, min.getLocalValue().longValue());
    }

    @Test
    public void testResetLocal() {
        LongMinimum min = new LongMinimum();
        long value = 9876543210L;

        min.add(value);
        assertEquals(value, min.getLocalValue().longValue());

        min.resetLocal();
        assertEquals(Long.MAX_VALUE, min.getLocalValue().longValue());
    }

    @Test
    public void testAdd() {
        LongMinimum min = new LongMinimum();

        min.add(1234567890);
        min.add(9876543210L);
        min.add(-9876543210L);
        min.add(-1234567890);

        assertEquals(-9876543210L, min.getLocalValue().longValue());
    }

    @Test
    public void testMerge() {
        LongMinimum min1 = new LongMinimum();
        min1.add(1234567890987654321L);

        LongMinimum min2 = new LongMinimum();
        min2.add(5678909876543210123L);

        min2.merge(min1);
        assertEquals(1234567890987654321L, min2.getLocalValue().longValue());

        min1.merge(min2);
        assertEquals(1234567890987654321L, min1.getLocalValue().longValue());
    }

    @Test
    public void testClone() {
        LongMinimum min = new LongMinimum();
        long value = 4242424242424242L;

        min.add(value);

        LongMinimum clone = min.clone();
        assertEquals(value, clone.getLocalValue().longValue());
    }
}
