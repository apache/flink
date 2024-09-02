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

package org.apache.flink.runtime.scheduler.adaptive;

import java.time.Instant;

/** Testing implementation for {@link StateTransitionManager}. */
public class TestingStateTransitionManager implements StateTransitionManager {

    private final Runnable onChangeRunnable;
    private final Runnable onTriggerRunnable;

    private TestingStateTransitionManager(Runnable onChangeRunnable, Runnable onTriggerRunnable) {
        this.onChangeRunnable = onChangeRunnable;
        this.onTriggerRunnable = onTriggerRunnable;
    }

    @Override
    public void onChange() {
        this.onChangeRunnable.run();
    }

    @Override
    public void onTrigger() {
        this.onTriggerRunnable.run();
    }

    /**
     * {@code Factory} implementation for creating {@code TestingStateTransitionManager} instances.
     */
    public static class Factory implements StateTransitionManager.Factory {

        private final Runnable onChangeRunnable;
        private final Runnable onTriggerRunnable;

        public static TestingStateTransitionManager.Factory noOpFactory() {
            return new Factory(() -> {}, () -> {});
        }

        public Factory(Runnable onChangeRunnable, Runnable onTriggerRunnable) {
            this.onChangeRunnable = onChangeRunnable;
            this.onTriggerRunnable = onTriggerRunnable;
        }

        @Override
        public StateTransitionManager create(Context ignoredContext, Instant ignoredLastRescale) {
            return new TestingStateTransitionManager(onChangeRunnable, onTriggerRunnable);
        }
    }
}
