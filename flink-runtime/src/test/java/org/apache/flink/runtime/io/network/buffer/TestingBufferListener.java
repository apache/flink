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

package org.apache.flink.runtime.io.network.buffer;

import java.util.function.Function;

/** Mock {@link BufferListener} for testing. */
public class TestingBufferListener implements BufferListener {

    private final Function<Buffer, Boolean> notifyBufferAvailableFunction;

    private final Runnable notifyBufferDestroyedRunnable;

    private TestingBufferListener(
            Function<Buffer, Boolean> notifyBufferAvailableFunction,
            Runnable notifyBufferDestroyedRunnable) {
        this.notifyBufferAvailableFunction = notifyBufferAvailableFunction;
        this.notifyBufferDestroyedRunnable = notifyBufferDestroyedRunnable;
    }

    @Override
    public boolean notifyBufferAvailable(Buffer buffer) {
        return notifyBufferAvailableFunction.apply(buffer);
    }

    @Override
    public void notifyBufferDestroyed() {
        notifyBufferDestroyedRunnable.run();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link TestingBufferListener}. */
    public static class Builder {
        private Function<Buffer, Boolean> notifyBufferAvailableFunction = (ignore) -> false;

        private Runnable notifyBufferDestroyedRunnable = () -> {};

        public Builder setNotifyBufferAvailableFunction(
                Function<Buffer, Boolean> notifyBufferAvailableFunction) {
            this.notifyBufferAvailableFunction = notifyBufferAvailableFunction;
            return this;
        }

        public Builder setNotifyBufferDestroyedRunnable(Runnable notifyBufferDestroyedRunnable) {
            this.notifyBufferDestroyedRunnable = notifyBufferDestroyedRunnable;
            return this;
        }

        public TestingBufferListener build() {
            return new TestingBufferListener(
                    notifyBufferAvailableFunction, notifyBufferDestroyedRunnable);
        }
    }
}
