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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

/** Test implementation for {@link TierConsumerAgent}. */
public class TestingTierConsumerAgent implements TierConsumerAgent {

    private final Runnable startNotifier;

    private final Supplier<Buffer> bufferSupplier;

    private final Runnable availabilityNotifierRegistrationRunnable;

    private final Runnable closeNotifier;

    private TestingTierConsumerAgent(
            Runnable startNotifier,
            Supplier<Buffer> bufferSupplier,
            Runnable availabilityNotifierRegistrationRunnable,
            Runnable closeNotifier) {
        this.startNotifier = startNotifier;
        this.bufferSupplier = bufferSupplier;
        this.availabilityNotifierRegistrationRunnable = availabilityNotifierRegistrationRunnable;
        this.closeNotifier = closeNotifier;
    }

    @Override
    public void start() {
        startNotifier.run();
    }

    @Override
    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        Buffer buffer = bufferSupplier.get();
        return buffer == null ? Optional.empty() : Optional.of(buffer);
    }

    @Override
    public void registerAvailabilityNotifier(AvailabilityNotifier notifier) {
        availabilityNotifierRegistrationRunnable.run();
    }

    @Override
    public void close() throws IOException {
        closeNotifier.run();
    }

    /** Builder for {@link TestingTierConsumerAgent}. */
    public static class Builder {

        private Runnable startNotifier = () -> {};

        private Supplier<Buffer> bufferSupplier = () -> null;

        private Runnable availabilityNotifierRegistrationRunnable = () -> {};

        private Runnable closeNotifier = () -> {};

        public Builder() {}

        public Builder setStartNotifier(Runnable startNotifier) {
            this.startNotifier = startNotifier;
            return this;
        }

        public Builder setBufferSupplier(Supplier<Buffer> bufferSupplier) {
            this.bufferSupplier = bufferSupplier;
            return this;
        }

        public Builder setAvailabilityNotifierRegistrationRunnable(
                Runnable availabilityNotifierRegistrationRunnable) {
            this.availabilityNotifierRegistrationRunnable =
                    availabilityNotifierRegistrationRunnable;
            return this;
        }

        public Builder setCloseNotifier(Runnable closeNotifier) {
            this.closeNotifier = closeNotifier;
            return this;
        }

        public TestingTierConsumerAgent build() {
            return new TestingTierConsumerAgent(
                    startNotifier,
                    bufferSupplier,
                    availabilityNotifierRegistrationRunnable,
                    closeNotifier);
        }
    }
}
