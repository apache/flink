/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io;

import org.apache.flink.annotation.Internal;

import java.util.concurrent.CompletableFuture;

/**
 * Interface defining couple of essential methods for listening on data availability using
 * {@link CompletableFuture}. For usage check out for example {@link PullingAsyncDataInput}.
 */
@Internal
public interface AvailabilityProvider {
	/**
	 * Constant that allows to avoid volatile checks {@link CompletableFuture#isDone()}. Check
	 * {@link #isAvailable()} and {@link #isApproximatelyAvailable()} for more explanation.
	 */
	CompletableFuture<?> AVAILABLE = CompletableFuture.completedFuture(null);

	/**
	 * @return a future that is completed if the respective provider is available.
	 */
	CompletableFuture<?> getAvailableFuture();

	/**
	 * In order to best-effort avoid volatile access in {@link CompletableFuture#isDone()}, we check the condition
	 * of <code>future == AVAILABLE</code> firstly for getting probable performance benefits while hot looping.
	 *
	 * <p>It is always safe to use this method in performance nonsensitive scenarios to get the precise state.
	 *
	 * @return true if this instance is available for further processing.
	 */
	default boolean isAvailable() {
		CompletableFuture<?> future = getAvailableFuture();
		return future == AVAILABLE || future.isDone();
	}

	/**
	 * Checks whether this instance is available only via constant {@link #AVAILABLE} to avoid
	 * performance concern caused by volatile access in {@link CompletableFuture#isDone()}. So it is
	 * mainly used in the performance sensitive scenarios which do not always need the precise state.
	 *
	 * <p>This method is still safe to get the precise state if {@link #getAvailableFuture()}
	 * was touched via (.get(), .wait(), .isDone(), ...) before, which also has a "happen-before"
	 * relationship with this call.
	 *
	 * @return true if this instance is available for further processing.
	 */
	default boolean isApproximatelyAvailable() {
		return getAvailableFuture() == AVAILABLE;
	}

	static CompletableFuture<?> and(CompletableFuture<?> first, CompletableFuture<?> second) {
		if (first == AVAILABLE && second == AVAILABLE) {
			return AVAILABLE;
		}
		else if (first == AVAILABLE) {
			return second;
		}
		else if (second == AVAILABLE) {
			return first;
		}
		else {
			return CompletableFuture.allOf(first, second);
		}
	}

	static CompletableFuture<?> or(CompletableFuture<?> first, CompletableFuture<?> second) {
		if (first == AVAILABLE || second == AVAILABLE) {
			return AVAILABLE;
		}
		return CompletableFuture.anyOf(first, second);
	}

	/**
	 * A availability implementation for providing the helpful functions of resetting the
	 * available/unavailable states.
	 */
	final class AvailabilityHelper implements AvailabilityProvider {

		private CompletableFuture<?> availableFuture = new CompletableFuture<>();

		public CompletableFuture<?> and(CompletableFuture<?> other) {
			return AvailabilityProvider.and(availableFuture, other);
		}

		public CompletableFuture<?> and(AvailabilityProvider other) {
			return and(other.getAvailableFuture());
		}

		public CompletableFuture<?> or(CompletableFuture<?> other) {
			return AvailabilityProvider.or(availableFuture, other);
		}

		public CompletableFuture<?> or(AvailabilityProvider other) {
			return or(other.getAvailableFuture());
		}

		/**
		 * Judges to reset the current available state as unavailable.
		 */
		public void resetUnavailable() {
			if (isAvailable()) {
				availableFuture = new CompletableFuture<>();
			}
		}

		/**
		 * Resets the constant completed {@link #AVAILABLE} as the current state.
		 */
		public void resetAvailable() {
			availableFuture = AVAILABLE;
		}

		/**
		 *  Returns the previously not completed future and resets the constant completed
		 *  {@link #AVAILABLE} as the current state.
		 */
		public CompletableFuture<?> getUnavailableToResetAvailable() {
			CompletableFuture<?> toNotify = availableFuture;
			availableFuture = AVAILABLE;
			return toNotify;
		}

		/**
		 *  Creates a new uncompleted future as the current state and returns the
		 *  previous uncompleted one.
		 */
		public CompletableFuture<?> getUnavailableToResetUnavailable() {
			CompletableFuture<?> toNotify = availableFuture;
			availableFuture = new CompletableFuture<>();
			return toNotify;
		}

		/**
		 * @return a future that is completed if the respective provider is available.
		 */
		@Override
		public CompletableFuture<?> getAvailableFuture() {
			return availableFuture;
		}
	}
}
