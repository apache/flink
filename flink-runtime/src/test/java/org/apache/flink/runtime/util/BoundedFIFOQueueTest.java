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

package org.apache.flink.runtime.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@code BoundedFIFOQueueTest} tests {@link BoundedFIFOQueue}. */
class BoundedFIFOQueueTest {

    @Test
    void testConstructorFailing() {
        assertThatThrownBy(() -> new BoundedFIFOQueue<>(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testQueueWithMaxSize0() {
        final BoundedFIFOQueue<Integer> testInstance = new BoundedFIFOQueue<>(0);
        assertThat(testInstance).isEmpty();
        testInstance.add(1);
        assertThat(testInstance).isEmpty();
    }

    @Test
    void testQueueWithMaxSize2() {
        final BoundedFIFOQueue<Integer> testInstance = new BoundedFIFOQueue<>(2);
        assertThat(testInstance).isEmpty();

        testInstance.add(1);
        assertThat(testInstance).contains(1);

        testInstance.add(2);
        assertThat(testInstance).contains(1, 2);

        testInstance.add(3);
        assertThat(testInstance).contains(2, 3);
    }

    @Test
    void testAddNullHandling() {
        final BoundedFIFOQueue<Integer> testInstance = new BoundedFIFOQueue<>(1);
        assertThatThrownBy(() -> testInstance.add(null))
                .withFailMessage("A NullPointerException is expected to be thrown.")
                .isInstanceOf(NullPointerException.class);

        assertThat(testInstance).isEmpty();
    }

    /**
     * Tests that {@link BoundedFIFOQueue#size()} returns the number of elements currently stored in
     * the queue with a {@code maxSize} of 0.
     */
    @Test
    void testSizeWithMaxSize0() {
        final BoundedFIFOQueue<Integer> testInstance = new BoundedFIFOQueue<>(0);
        assertThat(testInstance).isEmpty();

        testInstance.add(1);
        assertThat(testInstance).isEmpty();
    }

    /**
     * Tests that {@link BoundedFIFOQueue#size()} returns the number of elements currently stored in
     * the queue with a {@code maxSize} of 2.
     */
    @Test
    void testSizeWithMaxSize2() {
        final BoundedFIFOQueue<Integer> testInstance = new BoundedFIFOQueue<>(2);
        assertThat(testInstance).isEmpty();

        testInstance.add(5);
        assertThat(testInstance).hasSize(1);

        testInstance.add(6);
        assertThat(testInstance).hasSize(2);

        // adding a 3rd element won't increase the size anymore
        testInstance.add(7);
        assertThat(testInstance).hasSize(2);
    }
}
