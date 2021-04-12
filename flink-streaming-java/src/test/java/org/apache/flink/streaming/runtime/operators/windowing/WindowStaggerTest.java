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

import org.junit.Test;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for {@link WindowStagger}. */
public class WindowStaggerTest {

    @Test
    public void testWindowStagger() {
        long sizeInMilliseconds = 5000;

        assertEquals(0L, WindowStagger.ALIGNED.getStaggerOffset(500L, sizeInMilliseconds));
        assertEquals(500L, WindowStagger.NATURAL.getStaggerOffset(5500L, sizeInMilliseconds));
        assertThat(
                WindowStagger.RANDOM.getStaggerOffset(0L, sizeInMilliseconds),
                greaterThanOrEqualTo(0L));
        assertThat(
                WindowStagger.RANDOM.getStaggerOffset(0L, sizeInMilliseconds),
                lessThan(sizeInMilliseconds));
    }
}
