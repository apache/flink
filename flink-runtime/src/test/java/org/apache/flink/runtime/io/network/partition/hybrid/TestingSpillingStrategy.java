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

package org.apache.flink.runtime.io.network.partition.hybrid;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Mock {@link HsSpillingStrategy} for testing. */
public class TestingSpillingStrategy implements HsSpillingStrategy {
    private final BiFunction<Integer, Integer, Optional<Decision>> onMemoryUsageChangedFunction;

    private final Function<Integer, Optional<Decision>> onBufferFinishedFunction;

    private final Function<BufferIndexAndChannel, Optional<Decision>> onBufferConsumedFunction;

    private final Function<HsSpillingInfoProvider, Decision> decideActionWithGlobalInfoFunction;

    private final Function<HsSpillingInfoProvider, Decision> onResultPartitionClosedFunction;

    private TestingSpillingStrategy(
            BiFunction<Integer, Integer, Optional<Decision>> onMemoryUsageChangedFunction,
            Function<Integer, Optional<Decision>> onBufferFinishedFunction,
            Function<BufferIndexAndChannel, Optional<Decision>> onBufferConsumedFunction,
            Function<HsSpillingInfoProvider, Decision> decideActionWithGlobalInfoFunction,
            Function<HsSpillingInfoProvider, Decision> onResultPartitionClosedFunction) {
        this.onMemoryUsageChangedFunction = onMemoryUsageChangedFunction;
        this.onBufferFinishedFunction = onBufferFinishedFunction;
        this.onBufferConsumedFunction = onBufferConsumedFunction;
        this.decideActionWithGlobalInfoFunction = decideActionWithGlobalInfoFunction;
        this.onResultPartitionClosedFunction = onResultPartitionClosedFunction;
    }

    @Override
    public Optional<Decision> onMemoryUsageChanged(
            int numTotalRequestedBuffers, int currentPoolSize) {
        return onMemoryUsageChangedFunction.apply(numTotalRequestedBuffers, currentPoolSize);
    }

    @Override
    public Optional<Decision> onBufferFinished(int numTotalUnSpillBuffers) {
        return onBufferFinishedFunction.apply(numTotalUnSpillBuffers);
    }

    @Override
    public Optional<Decision> onBufferConsumed(BufferIndexAndChannel consumedBuffer) {
        return onBufferConsumedFunction.apply(consumedBuffer);
    }

    @Override
    public Decision decideActionWithGlobalInfo(HsSpillingInfoProvider spillingInfoProvider) {
        return decideActionWithGlobalInfoFunction.apply(spillingInfoProvider);
    }

    @Override
    public Decision onResultPartitionClosed(HsSpillingInfoProvider spillingInfoProvider) {
        return onResultPartitionClosedFunction.apply(spillingInfoProvider);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link TestingSpillingStrategy}. */
    public static class Builder {
        private BiFunction<Integer, Integer, Optional<Decision>> onMemoryUsageChangedFunction =
                (ignore1, ignore2) -> Optional.of(Decision.NO_ACTION);

        private Function<Integer, Optional<Decision>> onBufferFinishedFunction =
                (ignore) -> Optional.of(Decision.NO_ACTION);

        private Function<BufferIndexAndChannel, Optional<Decision>> onBufferConsumedFunction =
                (ignore) -> Optional.of(Decision.NO_ACTION);

        private Function<HsSpillingInfoProvider, Decision> decideActionWithGlobalInfoFunction =
                (ignore) -> Decision.NO_ACTION;

        private Function<HsSpillingInfoProvider, Decision> onResultPartitionClosedFunction =
                (ignore) -> Decision.NO_ACTION;

        private Builder() {}

        public Builder setOnMemoryUsageChangedFunction(
                BiFunction<Integer, Integer, Optional<Decision>> onMemoryUsageChangedFunction) {
            this.onMemoryUsageChangedFunction = onMemoryUsageChangedFunction;
            return this;
        }

        public Builder setOnBufferFinishedFunction(
                Function<Integer, Optional<Decision>> onBufferFinishedFunction) {
            this.onBufferFinishedFunction = onBufferFinishedFunction;
            return this;
        }

        public Builder setOnBufferConsumedFunction(
                Function<BufferIndexAndChannel, Optional<Decision>> onBufferConsumedFunction) {
            this.onBufferConsumedFunction = onBufferConsumedFunction;
            return this;
        }

        public Builder setDecideActionWithGlobalInfoFunction(
                Function<HsSpillingInfoProvider, Decision> decideActionWithGlobalInfoFunction) {
            this.decideActionWithGlobalInfoFunction = decideActionWithGlobalInfoFunction;
            return this;
        }

        public Builder setOnResultPartitionClosedFunction(
                Function<HsSpillingInfoProvider, Decision> onResultPartitionClosedFunction) {
            this.onResultPartitionClosedFunction = onResultPartitionClosedFunction;
            return this;
        }

        public TestingSpillingStrategy build() {
            return new TestingSpillingStrategy(
                    onMemoryUsageChangedFunction,
                    onBufferFinishedFunction,
                    onBufferConsumedFunction,
                    decideActionWithGlobalInfoFunction,
                    onResultPartitionClosedFunction);
        }
    }
}
