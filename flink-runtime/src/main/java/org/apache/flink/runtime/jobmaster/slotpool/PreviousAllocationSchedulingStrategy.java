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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * {@link SchedulingStrategy} which tries to match a slot with its previous {@link AllocationID}.
 * If the previous allocation cannot be found, then it returns {@code null}. If the slot has not
 * been scheduled before (no assigned allocation id), it will fall back to
 * {@link LocationPreferenceSchedulingStrategy}.
 */
public class PreviousAllocationSchedulingStrategy extends LocationPreferenceSchedulingStrategy {

	private static final PreviousAllocationSchedulingStrategy INSTANCE = new PreviousAllocationSchedulingStrategy();

	private PreviousAllocationSchedulingStrategy() {}

	@Nullable
	@Override
	public <IN, OUT> OUT findMatchWithLocality(
			@Nonnull SlotProfile slotProfile,
			@Nonnull Stream<IN> candidates,
			@Nonnull Function<IN, SlotContext> contextExtractor,
			@Nonnull Predicate<IN> additionalRequirementsFilter,
			@Nonnull BiFunction<IN, Locality, OUT> resultProducer) {

		Collection<AllocationID> priorAllocations = slotProfile.getPriorAllocations();

		if (priorAllocations.isEmpty()) {
			return super.findMatchWithLocality(slotProfile, candidates, contextExtractor, additionalRequirementsFilter, resultProducer);
		} else {
			return findPreviousAllocation(candidates, contextExtractor, additionalRequirementsFilter, resultProducer, priorAllocations);
		}
	}

	@Nullable
	private <IN, OUT> OUT findPreviousAllocation(
			@Nonnull Stream<IN> candidates,
			@Nonnull Function<IN, SlotContext> contextExtractor,
			@Nonnull Predicate<IN> additionalRequirementsFilter,
			@Nonnull BiFunction<IN, Locality, OUT> resultProducer,
			Collection<AllocationID> priorAllocations) {
		Predicate<IN> filterByAllocation =
			(candidate) -> priorAllocations.contains(contextExtractor.apply(candidate).getAllocationId());

		return candidates
			.filter(filterByAllocation.and(additionalRequirementsFilter))
			.findFirst()
			.map((result) -> resultProducer.apply(result, Locality.LOCAL)) // TODO introduce special locality?
			.orElse(null);
	}

	public static PreviousAllocationSchedulingStrategy getInstance() {
		return INSTANCE;
	}
}
