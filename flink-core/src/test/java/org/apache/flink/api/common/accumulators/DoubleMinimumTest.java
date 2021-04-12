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

public class DoubleMinimumTest {

    @Test
    public void testGet() {
        DoubleMinimum min = new DoubleMinimum();
        assertEquals(Double.POSITIVE_INFINITY, min.getLocalValue(), 0.0);
    }

    @Test
    public void testResetLocal() {
        DoubleMinimum min = new DoubleMinimum();
        double value = 13.57902468;

        min.add(value);
        assertEquals(value, min.getLocalValue(), 0.0);

        min.resetLocal();
        assertEquals(Double.POSITIVE_INFINITY, min.getLocalValue(), 0.0);
    }

    @Test
    public void testAdd() {
        DoubleMinimum min = new DoubleMinimum();

        min.add(1234.5768);
        min.add(9876.5432);
        min.add(-987.6543);
        min.add(-123.4567);

        assertEquals(-987.6543, min.getLocalValue(), 0.0);
    }

    @Test
    public void testMerge() {
        DoubleMinimum min1 = new DoubleMinimum();
        min1.add(1234.5768);

        DoubleMinimum min2 = new DoubleMinimum();
        min2.add(5678.9012);

        min2.merge(min1);
        assertEquals(1234.5768, min2.getLocalValue(), 0.0);

        min1.merge(min2);
        assertEquals(1234.5768, min1.getLocalValue(), 0.0);
    }

    @Test
    public void testClone() {
        DoubleMinimum min = new DoubleMinimum();
        double value = 3.14159265359;

        min.add(value);

        DoubleMinimum clone = min.clone();
        assertEquals(value, clone.getLocalValue(), 0.0);
    }
}
