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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class DoubleMinimumTest {

    @Test
    void testGet() {
        DoubleMinimum min = new DoubleMinimum();
        assertThat(min.getLocalValue()).isCloseTo(Double.POSITIVE_INFINITY, within(0.0));
    }

    @Test
    void testResetLocal() {
        DoubleMinimum min = new DoubleMinimum();
        double value = 13.57902468;

        min.add(value);
        assertThat(min.getLocalValue()).isCloseTo(value, within(0.0));

        min.resetLocal();
        assertThat(min.getLocalValue()).isCloseTo(Double.POSITIVE_INFINITY, within(0.0));
    }

    @Test
    void testAdd() {
        DoubleMinimum min = new DoubleMinimum();

        min.add(1234.5768);
        min.add(9876.5432);
        min.add(-987.6543);
        min.add(-123.4567);

        assertThat(min.getLocalValue()).isCloseTo(-987.6543, within(0.0));
    }

    @Test
    void testMerge() {
        DoubleMinimum min1 = new DoubleMinimum();
        min1.add(1234.5768);

        DoubleMinimum min2 = new DoubleMinimum();
        min2.add(5678.9012);

        min2.merge(min1);
        assertThat(min2.getLocalValue()).isCloseTo(1234.5768, within(0.0));

        min1.merge(min2);
        assertThat(min1.getLocalValue()).isCloseTo(1234.5768, within(0.0));
    }

    @Test
    void testClone() {
        DoubleMinimum min = new DoubleMinimum();
        double value = 3.14159265359;

        min.add(value);

        DoubleMinimum clone = min.clone();
        assertThat(clone.getLocalValue()).isCloseTo(value, within(0.0));
    }
}
