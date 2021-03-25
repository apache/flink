/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.util.ResourceCounter;

import java.util.Collection;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Testing implementation of {@link SlotAllocator}. */
public class TestingSlotAllocator implements SlotAllocator {

    private final Function<Iterable<JobInformation.VertexInformation>, ResourceCounter>
            calculateRequiredSlotsFunction;

    private final BiFunction<
                    JobInformation,
                    Collection<? extends SlotInfo>,
                    Optional<? extends VertexParallelism>>
            determineParallelismFunction;

    private final Function<VertexParallelism, Optional<ReservedSlots>> tryReserveResourcesFunction;

    private TestingSlotAllocator(
            Function<Iterable<JobInformation.VertexInformation>, ResourceCounter>
                    calculateRequiredSlotsFunction,
            BiFunction<
                            JobInformation,
                            Collection<? extends SlotInfo>,
                            Optional<? extends VertexParallelism>>
                    determineParallelismFunction,
            Function<VertexParallelism, Optional<ReservedSlots>> tryReserveResourcesFunction) {
        this.calculateRequiredSlotsFunction = calculateRequiredSlotsFunction;
        this.determineParallelismFunction = determineParallelismFunction;
        this.tryReserveResourcesFunction = tryReserveResourcesFunction;
    }

    @Override
    public ResourceCounter calculateRequiredSlots(
            Iterable<JobInformation.VertexInformation> vertices) {
        return calculateRequiredSlotsFunction.apply(vertices);
    }

    @Override
    public Optional<? extends VertexParallelism> determineParallelism(
            JobInformation jobInformation, Collection<? extends SlotInfo> slots) {
        return determineParallelismFunction.apply(jobInformation, slots);
    }

    @Override
    public Optional<ReservedSlots> tryReserveResources(VertexParallelism vertexParallelism) {
        return tryReserveResourcesFunction.apply(vertexParallelism);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for the {@link TestingSlotAllocator}. */
    public static final class Builder {
        private Function<Iterable<JobInformation.VertexInformation>, ResourceCounter>
                calculateRequiredSlotsFunction = ignored -> ResourceCounter.empty();
        private BiFunction<
                        JobInformation,
                        Collection<? extends SlotInfo>,
                        Optional<? extends VertexParallelism>>
                determineParallelismFunction = (ignoredA, ignoredB) -> Optional.empty();
        private Function<VertexParallelism, Optional<ReservedSlots>> tryReserveResourcesFunction =
                ignored -> Optional.empty();

        public Builder setCalculateRequiredSlotsFunction(
                Function<Iterable<JobInformation.VertexInformation>, ResourceCounter>
                        calculateRequiredSlotsFunction) {
            this.calculateRequiredSlotsFunction = calculateRequiredSlotsFunction;
            return this;
        }

        public Builder setDetermineParallelismFunction(
                BiFunction<
                                JobInformation,
                                Collection<? extends SlotInfo>,
                                Optional<? extends VertexParallelism>>
                        determineParallelismFunction) {
            this.determineParallelismFunction = determineParallelismFunction;
            return this;
        }

        public Builder setTryReserveResourcesFunction(
                Function<VertexParallelism, Optional<ReservedSlots>> tryReserveResourcesFunction) {
            this.tryReserveResourcesFunction = tryReserveResourcesFunction;
            return this;
        }

        public TestingSlotAllocator build() {
            return new TestingSlotAllocator(
                    calculateRequiredSlotsFunction,
                    determineParallelismFunction,
                    tryReserveResourcesFunction);
        }
    }
}
