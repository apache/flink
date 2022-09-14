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

package org.apache.flink.core.testutils;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the OneShotLatch. */
class OneShotLatchTest {

    @Test
    void testAwaitWithTimeout() throws Exception {
        OneShotLatch latch = new OneShotLatch();
        assertThat(latch.isTriggered()).isFalse();

        assertThatThrownBy(() -> latch.await(1, TimeUnit.MILLISECONDS))
                .withFailMessage(() -> "should fail with a TimeoutException")
                .isInstanceOf(TimeoutException.class);

        assertThat(latch.isTriggered()).isFalse();

        latch.trigger();
        assertThat(latch.isTriggered()).isTrue();

        latch.await(100, TimeUnit.DAYS);
        assertThat(latch.isTriggered()).isTrue();

        latch.await(0, TimeUnit.MILLISECONDS);
        assertThat(latch.isTriggered()).isTrue();
    }
}
