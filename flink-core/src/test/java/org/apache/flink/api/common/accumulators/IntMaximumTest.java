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

public class IntMaximumTest {

    @Test
    public void testGet() {
        IntMaximum max = new IntMaximum();
        assertEquals(Integer.MIN_VALUE, max.getLocalValue().intValue());
    }

    @Test
    public void testResetLocal() {
        IntMaximum max = new IntMaximum();
        int value = 13;

        max.add(value);
        assertEquals(value, max.getLocalValue().intValue());

        max.resetLocal();
        assertEquals(Integer.MIN_VALUE, max.getLocalValue().intValue());
    }

    @Test
    public void testAdd() {
        IntMaximum max = new IntMaximum();

        max.add(1234);
        max.add(9876);
        max.add(-987);
        max.add(-123);

        assertEquals(9876, max.getLocalValue().intValue());
    }

    @Test
    public void testMerge() {
        IntMaximum max1 = new IntMaximum();
        max1.add(1234);

        IntMaximum max2 = new IntMaximum();
        max2.add(5678);

        max2.merge(max1);
        assertEquals(5678, max2.getLocalValue().intValue());

        max1.merge(max2);
        assertEquals(5678, max1.getLocalValue().intValue());
    }

    @Test
    public void testClone() {
        IntMaximum max = new IntMaximum();
        int value = 42;

        max.add(value);

        IntMaximum clone = max.clone();
        assertEquals(value, clone.getLocalValue().intValue());
    }
}
