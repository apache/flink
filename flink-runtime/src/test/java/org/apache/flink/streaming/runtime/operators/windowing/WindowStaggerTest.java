/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link WindowStagger}. */
class WindowStaggerTest {

    @Test
    void testWindowStagger() {
        long sizeInMilliseconds = 5000;

        assertThat(WindowStagger.ALIGNED.getStaggerOffset(500L, sizeInMilliseconds)).isZero();
        assertThat(WindowStagger.NATURAL.getStaggerOffset(5500L, sizeInMilliseconds))
                .isEqualTo(500L);
        assertThat(WindowStagger.RANDOM.getStaggerOffset(0L, sizeInMilliseconds))
                .isGreaterThanOrEqualTo(0L);
        assertThat(WindowStagger.RANDOM.getStaggerOffset(0L, sizeInMilliseconds))
                .isLessThan(sizeInMilliseconds);
    }
}
