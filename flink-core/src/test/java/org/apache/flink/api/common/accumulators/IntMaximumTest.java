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

class IntMaximumTest {

    @Test
    void testGet() {
        IntMaximum max = new IntMaximum();
        assertThat(max.getLocalValue().intValue()).isEqualTo(Integer.MIN_VALUE);
    }

    @Test
    void testResetLocal() {
        IntMaximum max = new IntMaximum();
        int value = 13;

        max.add(value);
        assertThat(max.getLocalValue().intValue()).isEqualTo(value);

        max.resetLocal();
        assertThat(max.getLocalValue().intValue()).isEqualTo(Integer.MIN_VALUE);
    }

    @Test
    void testAdd() {
        IntMaximum max = new IntMaximum();

        max.add(1234);
        max.add(9876);
        max.add(-987);
        max.add(-123);

        assertThat(max.getLocalValue().intValue()).isEqualTo(9876);
    }

    @Test
    void testMerge() {
        IntMaximum max1 = new IntMaximum();
        max1.add(1234);

        IntMaximum max2 = new IntMaximum();
        max2.add(5678);

        max2.merge(max1);
        assertThat(max2.getLocalValue().intValue()).isEqualTo(5678);

        max1.merge(max2);
        assertThat(max1.getLocalValue().intValue()).isEqualTo(5678);
    }

    @Test
    void testClone() {
        IntMaximum max = new IntMaximum();
        int value = 42;

        max.add(value);

        IntMaximum clone = max.clone();
        assertThat(clone.getLocalValue().intValue()).isEqualTo(value);
    }
}
