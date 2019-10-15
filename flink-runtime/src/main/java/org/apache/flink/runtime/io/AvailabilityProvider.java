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
	 * {@link #isAvailable()} for more explanation.
	 */
	CompletableFuture<?> AVAILABLE = CompletableFuture.completedFuture(null);

	/**
	 * Check if this instance is available for further processing.
	 *
	 * <p>When hot looping to avoid volatile access in {@link CompletableFuture#isDone()} user of
	 * this method should do the following check:
	 * <pre>
	 * {@code
	 *	AvailabilityProvider input = ...;
	 *	if (input.isAvailable() == AvailabilityProvider.AVAILABLE || input.isAvailable().isDone()) {
	 *		// do something;
	 *	}
	 * }
	 * </pre>
	 *
	 * @return a future that is completed if there are more records available. If there are more
	 * records available immediately, {@link #AVAILABLE} should be returned. Previously returned
	 * not completed futures should become completed once there is more input available or if
	 * the input is finished.
	 */
	CompletableFuture<?> isAvailable();

	/**
	 * A availability implementation for providing the helpful functions of resetting the
	 * available/unavailable states.
	 */
	final class AvailabilityHelper implements AvailabilityProvider {

		private CompletableFuture<?> isAvailable = new CompletableFuture<>();

		/**
		 * Judges to reset the current available state as unavailable.
		 */
		public void resetUnavailable() {
			// try to avoid volatile access in isDone()}
			if (isAvailable == AVAILABLE || isAvailable.isDone()) {
				isAvailable = new CompletableFuture<>();
			}
		}

		/**
		 * Resets the constant completed {@link #AVAILABLE} as the current state.
		 */
		public void resetAvailable() {
			isAvailable = AVAILABLE;
		}

		/**
		 *  Returns the previously not completed future and resets the constant completed
		 *  {@link #AVAILABLE} as the current state.
		 */
		public CompletableFuture<?> getUnavailableToResetAvailable() {
			CompletableFuture<?> toNotify = isAvailable;
			isAvailable = AVAILABLE;
			return toNotify;
		}

		/**
		 * @return a future that is completed if the respective provider is available.
		 */
		@Override
		public CompletableFuture<?> isAvailable() {
			return isAvailable;
		}
	}
}
