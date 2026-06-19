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
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.function.TriFunction;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Testing implementation of {@link SlotAllocator}. */
public class TestingSlotAllocator implements SlotAllocator {

    private final Function<Iterable<JobInformation.VertexInformation>, ResourceCounter>
            calculateRequiredSlotsFunction;

    private final Function<JobSchedulingPlan, Optional<ReservedSlots>> tryReserveResourcesFunction;

    private final BiFunction<
                    JobInformation, Collection<? extends SlotInfo>, Optional<VertexParallelism>>
            determineParallelismFunction;

    private final TriFunction<
                    JobInformation,
                    Collection<? extends SlotInfo>,
                    JobAllocationsInformation,
                    Optional<JobSchedulingPlan>>
            determineParallelismAndCalculateAssignmentFunction;

    private TestingSlotAllocator(
            Function<Iterable<JobInformation.VertexInformation>, ResourceCounter>
                    calculateRequiredSlotsFunction,
            Function<JobSchedulingPlan, Optional<ReservedSlots>> tryReserveResourcesFunction,
            final BiFunction<
                            JobInformation,
                            Collection<? extends SlotInfo>,
                            Optional<VertexParallelism>>
                    determineParallelismFunction,
            final TriFunction<
                            JobInformation,
                            Collection<? extends SlotInfo>,
                            JobAllocationsInformation,
                            Optional<JobSchedulingPlan>>
                    determineParallelismAndCalculateAssignmentFunction) {
        this.calculateRequiredSlotsFunction = calculateRequiredSlotsFunction;
        this.tryReserveResourcesFunction = tryReserveResourcesFunction;
        this.determineParallelismFunction = determineParallelismFunction;
        this.determineParallelismAndCalculateAssignmentFunction =
                determineParallelismAndCalculateAssignmentFunction;
    }

    @Override
    public ResourceCounter calculateRequiredSlots(
            Iterable<JobInformation.VertexInformation> vertices) {
        return calculateRequiredSlotsFunction.apply(vertices);
    }

    @Override
    public Optional<VertexParallelism> determineParallelism(
            JobInformation jobInformation, Collection<? extends SlotInfo> slots) {
        return determineParallelismFunction.apply(jobInformation, slots);
    }

    @Override
    public Optional<JobSchedulingPlan> determineParallelismAndCalculateAssignment(
            JobInformation jobInformation,
            Collection<PhysicalSlot> slots,
            JobAllocationsInformation jobAllocationsInformation) {
        return determineParallelismAndCalculateAssignmentFunction.apply(
                jobInformation, slots, jobAllocationsInformation);
    }

    @Override
    public Optional<ReservedSlots> tryReserveResources(JobSchedulingPlan jobSchedulingPlan) {
        return tryReserveResourcesFunction.apply(jobSchedulingPlan);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for the {@link TestingSlotAllocator}. */
    public static final class Builder {
        private Function<Iterable<JobInformation.VertexInformation>, ResourceCounter>
                calculateRequiredSlotsFunction = ignored -> ResourceCounter.empty();
        private Function<JobSchedulingPlan, Optional<ReservedSlots>> tryReserveResourcesFunction =
                ignored -> Optional.empty();

        private BiFunction<
                        JobInformation, Collection<? extends SlotInfo>, Optional<VertexParallelism>>
                determineSlotsFunction = (jobInformation, slots) -> Optional.empty();

        private TriFunction<
                        JobInformation,
                        Collection<? extends SlotInfo>,
                        JobAllocationsInformation,
                        Optional<JobSchedulingPlan>>
                determineParallelismAndCalculateAssignmentFunction =
                        (jobInformation, slots, jobAllocationsInformation) -> Optional.empty();

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
                                Optional<VertexParallelism>>
                        determineParallelismFunction) {
            this.determineSlotsFunction = determineParallelismFunction;
            return this;
        }

        public Builder setDetermineParallelismAndCalculateAssignmentFunction(
                TriFunction<
                                JobInformation,
                                Collection<? extends SlotInfo>,
                                JobAllocationsInformation,
                                Optional<JobSchedulingPlan>>
                        determineParallelismAndCalculateAssignmentFunction) {
            this.determineParallelismAndCalculateAssignmentFunction =
                    determineParallelismAndCalculateAssignmentFunction;
            return this;
        }

        public Builder setTryReserveResourcesFunction(
                Function<JobSchedulingPlan, Optional<ReservedSlots>> tryReserveResourcesFunction) {
            this.tryReserveResourcesFunction = tryReserveResourcesFunction;
            return this;
        }

        public TestingSlotAllocator build() {
            return new TestingSlotAllocator(
                    calculateRequiredSlotsFunction,
                    tryReserveResourcesFunction,
                    determineSlotsFunction,
                    determineParallelismAndCalculateAssignmentFunction);
        }
    }

    public static TestingSlotAllocator getArgumentCapturingDelegatingSlotAllocator(
            final SlotAllocator slotAllocator,
            final List<JobAllocationsInformation> capturedAllocations) {
        return TestingSlotAllocator.newBuilder()
                .setCalculateRequiredSlotsFunction(slotAllocator::calculateRequiredSlots)
                .setTryReserveResourcesFunction(slotAllocator::tryReserveResources)
                .setDetermineParallelismFunction(slotAllocator::determineParallelism)
                .setDetermineParallelismAndCalculateAssignmentFunction(
                        (jobInformation, slotInfos, jobAllocationsInformation) -> {
                            capturedAllocations.add(jobAllocationsInformation);
                            return slotAllocator.determineParallelismAndCalculateAssignment(
                                    jobInformation,
                                    slotInfos.stream()
                                            .map(si -> (PhysicalSlot) si)
                                            .collect(Collectors.toList()),
                                    jobAllocationsInformation);
                        })
                .build();
    }
}
