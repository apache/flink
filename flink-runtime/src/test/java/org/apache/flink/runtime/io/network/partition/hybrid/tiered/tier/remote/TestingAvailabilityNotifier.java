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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/** Test implementation for {@link AvailabilityNotifier}. */
public class TestingAvailabilityNotifier implements AvailabilityNotifier {

    private final BiFunction<
                    TieredStoragePartitionId, TieredStorageSubpartitionId, CompletableFuture<Void>>
            notifyFunction;

    public TestingAvailabilityNotifier(
            BiFunction<
                            TieredStoragePartitionId,
                            TieredStorageSubpartitionId,
                            CompletableFuture<Void>>
                    notifyFunction) {
        this.notifyFunction = notifyFunction;
    }

    @Override
    public void notifyAvailable(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
        notifyFunction.apply(partitionId, subpartitionId).complete(null);
    }

    /** Builder for {@link TestingAvailabilityNotifier}. */
    public static class Builder {

        private BiFunction<
                        TieredStoragePartitionId,
                        TieredStorageSubpartitionId,
                        CompletableFuture<Void>>
                notifyFunction = (partitionId, subpartitionId) -> new CompletableFuture<>();

        public Builder() {}

        public Builder setNotifyFunction(
                BiFunction<
                                TieredStoragePartitionId,
                                TieredStorageSubpartitionId,
                                CompletableFuture<Void>>
                        notifyFunction) {
            this.notifyFunction = notifyFunction;
            return this;
        }

        public TestingAvailabilityNotifier build() {
            return new TestingAvailabilityNotifier(notifyFunction);
        }
    }
}
