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

package org.apache.flink.datastream.impl.context;

import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.TestInternalTimerService;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultProcessingTimeManager}. */
class DefaultProcessingTimeManagerTest {
    @Test
    void testCurrentProcessingTime() throws Exception {
        TestInternalTimerService<Integer, VoidNamespace> timerService = getTimerService();
        DefaultProcessingTimeManager manager = new DefaultProcessingTimeManager(timerService);
        long newTime = 100L;
        timerService.advanceProcessingTime(newTime);
        assertThat(manager.currentTime()).isEqualTo(newTime);
    }

    @Test
    void testRegisterProcessingTimer() {
        TestInternalTimerService<Integer, VoidNamespace> timerService = getTimerService();
        DefaultProcessingTimeManager manager = new DefaultProcessingTimeManager(timerService);
        assertThat(timerService.numProcessingTimeTimers()).isZero();
        manager.registerTimer(100L);
        assertThat(timerService.numProcessingTimeTimers()).isOne();
    }

    @Test
    void testDeleteProcessingTimeTimer() {
        TestInternalTimerService<Integer, VoidNamespace> timerService = getTimerService();
        DefaultProcessingTimeManager manager = new DefaultProcessingTimeManager(timerService);
        long time = 100L;
        manager.registerTimer(time);
        assertThat(timerService.numProcessingTimeTimers()).isOne();
        manager.deleteTimer(time);
        assertThat(timerService.numProcessingTimeTimers()).isZero();
    }

    @NotNull
    private static TestInternalTimerService<Integer, VoidNamespace> getTimerService() {
        return new TestInternalTimerService<>(
                new KeyContext() {
                    @Override
                    public void setCurrentKey(Object key) {}

                    @Override
                    public Object getCurrentKey() {
                        return "key";
                    }
                });
    }
}
