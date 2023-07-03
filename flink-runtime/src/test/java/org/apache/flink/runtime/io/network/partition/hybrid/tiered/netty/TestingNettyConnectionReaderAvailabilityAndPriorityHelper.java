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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import java.util.function.BiConsumer;

/** Test implementation for {@link NettyConnectionReaderAvailabilityAndPriorityHelper}. */
public class TestingNettyConnectionReaderAvailabilityAndPriorityHelper
        implements NettyConnectionReaderAvailabilityAndPriorityHelper {

    private final BiConsumer<Integer, Boolean> availableAndPriorityConsumer;

    private final BiConsumer<Integer, Integer> prioritySequenceNumberConsumer;

    private TestingNettyConnectionReaderAvailabilityAndPriorityHelper(
            BiConsumer<Integer, Boolean> availableAndPriorityConsumer,
            BiConsumer<Integer, Integer> prioritySequenceNumberConsumer) {
        this.availableAndPriorityConsumer = availableAndPriorityConsumer;
        this.prioritySequenceNumberConsumer = prioritySequenceNumberConsumer;
    }

    @Override
    public void notifyReaderAvailableAndPriority(int channelIndex, boolean isPriority) {
        availableAndPriorityConsumer.accept(channelIndex, isPriority);
    }

    @Override
    public void updatePrioritySequenceNumber(int channelIndex, int sequenceNumber) {
        prioritySequenceNumberConsumer.accept(channelIndex, sequenceNumber);
    }

    /** Builder for {@link TestingNettyConnectionReaderAvailabilityAndPriorityHelper}. */
    public static class Builder {

        private BiConsumer<Integer, Boolean> availableAndPriorityConsumer =
                (channelIndex, isPriority) -> {};

        private BiConsumer<Integer, Integer> prioritySequenceNumberConsumer =
                (channelIndex, sequenceNumber) -> {};

        public Builder() {}

        public Builder setAvailableAndPriorityConsumer(
                BiConsumer<Integer, Boolean> availableAndPriorityConsumer) {
            this.availableAndPriorityConsumer = availableAndPriorityConsumer;
            return this;
        }

        public Builder setPrioritySequenceNumberConsumer(
                BiConsumer<Integer, Integer> prioritySequenceNumberConsumer) {
            this.prioritySequenceNumberConsumer = prioritySequenceNumberConsumer;
            return this;
        }

        public TestingNettyConnectionReaderAvailabilityAndPriorityHelper build() {
            return new TestingNettyConnectionReaderAvailabilityAndPriorityHelper(
                    availableAndPriorityConsumer, prioritySequenceNumberConsumer);
        }
    }
}
