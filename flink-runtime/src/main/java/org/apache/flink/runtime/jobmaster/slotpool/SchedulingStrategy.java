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

import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotInfo;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Classes that implement this interface provide a method to match objects to somehow represent slot candidates
 * against the {@link SlotProfile} that produced the matcher object. A matching candidate is transformed into a
 * desired result. If the matcher does not find a matching candidate, it returns null.
 */
public interface SchedulingStrategy {

	/**
	 * This method takes the candidate slots, extracts slot contexts from them, filters them by the profile
	 * requirements and potentially by additional requirements, and produces a result from a match.
	 *
	 * @param slotProfile slotProfile for which to find a matching slot
	 * @param candidates stream of candidates to match against.
	 * @param contextExtractor function to extract the {@link SlotContext} from the candidates.
	 * @param additionalRequirementsFilter predicate to specify additional requirements for each candidate.
	 * @param resultProducer function to produce a result from a matching candidate input.
	 * @param <IN> type of the objects against we match the profile.
	 * @param <OUT> type of the produced output from a matching object.
	 * @return the result produced by resultProducer if a matching candidate was found or null otherwise.
	 */
	@Nullable
	<IN, OUT> OUT findMatchWithLocality(
		@Nonnull SlotProfile slotProfile,
		@Nonnull Supplier<Stream<IN>> candidates,
		@Nonnull Function<IN, SlotInfo> contextExtractor,
		@Nonnull Predicate<IN> additionalRequirementsFilter,
		@Nonnull BiFunction<IN, Locality, OUT> resultProducer);
}
