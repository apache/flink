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

class DoubleMaximumTest {

    @Test
    void testGet() {
        DoubleMaximum max = new DoubleMaximum();
        assertThat(max.getLocalValue()).isCloseTo(Double.NEGATIVE_INFINITY, within(0.0));
    }

    @Test
    void testResetLocal() {
        DoubleMaximum max = new DoubleMaximum();
        double value = 13.57902468;

        max.add(value);
        assertThat(max.getLocalValue()).isCloseTo(value, within(0.0));

        max.resetLocal();
        assertThat(max.getLocalValue()).isCloseTo(Double.NEGATIVE_INFINITY, within(0.0));
    }

    @Test
    void testAdd() {
        DoubleMaximum max = new DoubleMaximum();

        max.add(1234.5768);
        max.add(9876.5432);
        max.add(-987.6543);
        max.add(-123.4567);

        assertThat(max.getLocalValue()).isCloseTo(9876.5432, within(0.0));
    }

    @Test
    void testMerge() {
        DoubleMaximum max1 = new DoubleMaximum();
        max1.add(1234.5768);

        DoubleMaximum max2 = new DoubleMaximum();
        max2.add(5678.9012);

        max2.merge(max1);
        assertThat(max2.getLocalValue()).isCloseTo(5678.9012, within(0.0));

        max1.merge(max2);
        assertThat(max1.getLocalValue()).isCloseTo(5678.9012, within(0.0));
    }

    @Test
    void testClone() {
        DoubleMaximum max = new DoubleMaximum();
        double value = 3.14159265359;

        max.add(value);

        DoubleMaximum clone = max.clone();
        assertThat(clone.getLocalValue()).isCloseTo(value, within(0.0));
    }
}
