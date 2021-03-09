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

package org.apache.flink.api.common.operators;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class OrderingTest {

    @Test
    public void testNewOrdering() {
        Ordering ordering = new Ordering();

        // add a field
        ordering.appendOrdering(3, Integer.class, Order.ASCENDING);
        assertEquals(1, ordering.getNumberOfFields());

        // add a second field
        ordering.appendOrdering(1, Long.class, Order.DESCENDING);
        assertEquals(2, ordering.getNumberOfFields());

        // duplicate field index does not change Ordering
        ordering.appendOrdering(1, String.class, Order.ASCENDING);
        assertEquals(2, ordering.getNumberOfFields());

        // verify field positions, types, and orderings
        assertArrayEquals(new int[] {3, 1}, ordering.getFieldPositions());
        assertArrayEquals(new Class[] {Integer.class, Long.class}, ordering.getTypes());
        assertArrayEquals(new boolean[] {true, false}, ordering.getFieldSortDirections());
    }
}
