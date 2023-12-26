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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

import java.util.function.BiFunction;

/** Test implementation for {@link TierProducerAgent}. */
public class TestingTierProducerAgent implements TierProducerAgent {

    private final BiFunction<TieredStorageSubpartitionId, Integer, Boolean>
            tryStartNewSegmentSupplier;

    private final BiFunction<TieredStorageSubpartitionId, Buffer, Boolean> tryWriterFunction;

    private final Runnable closeRunnable;

    private TestingTierProducerAgent(
            BiFunction<TieredStorageSubpartitionId, Integer, Boolean> tryStartNewSegmentSupplier,
            BiFunction<TieredStorageSubpartitionId, Buffer, Boolean> tryWriterFunction,
            Runnable closeRunnable) {
        this.tryStartNewSegmentSupplier = tryStartNewSegmentSupplier;
        this.tryWriterFunction = tryWriterFunction;
        this.closeRunnable = closeRunnable;
    }

    public static TestingTierProducerAgent.Builder builder() {
        return new TestingTierProducerAgent.Builder();
    }

    @Override
    public boolean tryStartNewSegment(TieredStorageSubpartitionId subpartitionId, int segmentId) {
        return tryStartNewSegmentSupplier.apply(subpartitionId, segmentId);
    }

    @Override
    public boolean tryWrite(
            TieredStorageSubpartitionId subpartitionId, Buffer finishedBuffer, Object bufferOwner) {
        return tryWriterFunction.apply(subpartitionId, finishedBuffer);
    }

    @Override
    public void close() {
        closeRunnable.run();
    }

    /** Builder for {@link TierProducerAgent}. */
    public static class Builder {
        private BiFunction<TieredStorageSubpartitionId, Integer, Boolean> tryStartSegmentSupplier =
                (subpartitionId, integer) -> true;

        private BiFunction<TieredStorageSubpartitionId, Buffer, Boolean> tryWriterFunction =
                (subpartitionId, buffer) -> true;

        private Runnable closeRunnable = () -> {};

        public Builder() {}

        public TestingTierProducerAgent.Builder setTryStartSegmentSupplier(
                BiFunction<TieredStorageSubpartitionId, Integer, Boolean> tryStartSegmentSupplier) {
            this.tryStartSegmentSupplier = tryStartSegmentSupplier;
            return this;
        }

        public TestingTierProducerAgent.Builder setTryWriterFunction(
                BiFunction<TieredStorageSubpartitionId, Buffer, Boolean> tryWriterFunction) {
            this.tryWriterFunction = tryWriterFunction;
            return this;
        }

        public TestingTierProducerAgent.Builder setCloseRunnable(Runnable closeRunnable) {
            this.closeRunnable = closeRunnable;
            return this;
        }

        public TestingTierProducerAgent build() {
            return new TestingTierProducerAgent(
                    tryStartSegmentSupplier, tryWriterFunction, closeRunnable);
        }
    }
}
