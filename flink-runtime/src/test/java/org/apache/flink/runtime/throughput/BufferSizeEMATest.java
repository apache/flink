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

package org.apache.flink.runtime.throughput;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** Test for {@link BufferSizeEMA}. */
public class BufferSizeEMATest extends TestLogger {

    @Test
    public void testCalculationBufferSize() {
        BufferSizeEMA calculator = new BufferSizeEMA(200, 10, 3);

        // The result value seeks to the bottom limit but it will take a while until it reaches it.
        assertThat(calculator.calculateBufferSize(111, 13), is(104));
        assertThat(calculator.calculateBufferSize(107, 7), is(59));
        assertThat(calculator.calculateBufferSize(107, 7), is(37));
        assertThat(calculator.calculateBufferSize(107, 7), is(26));
        assertThat(calculator.calculateBufferSize(107, 7), is(20));
        assertThat(calculator.calculateBufferSize(107, 7), is(17));
        assertThat(calculator.calculateBufferSize(107, 13), is(12));
        assertThat(calculator.calculateBufferSize(107, 13), is(10));

        // Upgrade
        assertThat(calculator.calculateBufferSize(333, 1), is(15));
        assertThat(calculator.calculateBufferSize(333, 1), is(22));
        assertThat(calculator.calculateBufferSize(333, 1), is(33));
        assertThat(calculator.calculateBufferSize(333, 1), is(49));
        assertThat(calculator.calculateBufferSize(333, 1), is(73));
        assertThat(calculator.calculateBufferSize(333, 1), is(109));
    }

    @Test
    public void testSizeGreaterThanMaxSize() {
        BufferSizeEMA calculator = new BufferSizeEMA(200, 10, 3);

        // Decrease value to less than max.
        assertThat(calculator.calculateBufferSize(0, 1), is(100));

        // Impossible to exceed maximum.
        assertThat(calculator.calculateBufferSize(1000, 1), is(150));
        assertThat(calculator.calculateBufferSize(1000, 1), is(200));
        assertThat(calculator.calculateBufferSize(1000, 1), is(200));
    }

    @Test
    public void testSizeLessThanMinSize() {
        BufferSizeEMA calculator = new BufferSizeEMA(200, 10, 3);

        // Impossible to less than min.
        assertThat(calculator.calculateBufferSize(0, 1), is(100));
        assertThat(calculator.calculateBufferSize(0, 1), is(50));
        assertThat(calculator.calculateBufferSize(0, 1), is(25));
        assertThat(calculator.calculateBufferSize(0, 1), is(12));
        assertThat(calculator.calculateBufferSize(0, 1), is(10));
        assertThat(calculator.calculateBufferSize(0, 1), is(10));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeTotalSize() {
        BufferSizeEMA calculator = new BufferSizeEMA(100, 200, 2);
        calculator.calculateBufferSize(-1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZeroBuffers() {
        BufferSizeEMA calculator = new BufferSizeEMA(100, 200, 2);
        calculator.calculateBufferSize(1, 0);
    }
}
