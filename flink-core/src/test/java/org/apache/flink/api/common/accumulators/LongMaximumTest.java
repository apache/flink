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

class LongMaximumTest {

    @Test
    void testGet() {
        LongMaximum max = new LongMaximum();
        assertThat(max.getLocalValue().longValue()).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void testResetLocal() {
        LongMaximum max = new LongMaximum();
        long value = 9876543210L;

        max.add(value);
        assertThat(max.getLocalValue().longValue()).isEqualTo(value);

        max.resetLocal();
        assertThat(max.getLocalValue().longValue()).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void testAdd() {
        LongMaximum max = new LongMaximum();

        max.add(1234567890);
        max.add(9876543210L);
        max.add(-9876543210L);
        max.add(-1234567890);

        assertThat(max.getLocalValue().longValue()).isEqualTo(9876543210L);
    }

    @Test
    void testMerge() {
        LongMaximum max1 = new LongMaximum();
        max1.add(1234567890987654321L);

        LongMaximum max2 = new LongMaximum();
        max2.add(5678909876543210123L);

        max2.merge(max1);
        assertThat(max2.getLocalValue().longValue()).isEqualTo(5678909876543210123L);

        max1.merge(max2);
        assertThat(max1.getLocalValue().longValue()).isEqualTo(5678909876543210123L);
    }

    @Test
    void testClone() {
        LongMaximum max = new LongMaximum();
        long value = 4242424242424242L;

        max.add(value);

        LongMaximum clone = max.clone();
        assertThat(clone.getLocalValue().longValue()).isEqualTo(value);
    }
}
