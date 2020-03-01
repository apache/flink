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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.util.Preconditions;

/**
 * Budget manager for {@link ResourceProfile}.
 *
 * <p>For a given total resource budget, this class handles reserving and releasing resources
 * from the budget, and rejects reservations if they cannot be satisfied by the remaining budget.
 *
 * <p>Both the total budget and the reservations are in the form of {@link ResourceProfile}.
 */
public class ResourceBudgetManager {

	private final ResourceProfile totalBudget;

	private ResourceProfile availableBudget;

	public ResourceBudgetManager(final ResourceProfile totalBudget) {
		checkResourceProfileNotNullOrUnknown(totalBudget);
		this.totalBudget = totalBudget;
		this.availableBudget = totalBudget;
	}

	public ResourceProfile getTotalBudget() {
		return totalBudget;
	}

	public ResourceProfile getAvailableBudget() {
		return availableBudget;
	}

	public boolean reserve(final ResourceProfile reservation) {
		checkResourceProfileNotNullOrUnknown(reservation);
		if (!availableBudget.isMatching(reservation)) {
			return false;
		}

		availableBudget = availableBudget.subtract(reservation);
		return true;
	}

	public boolean release(final ResourceProfile reservation) {
		checkResourceProfileNotNullOrUnknown(reservation);
		ResourceProfile newAvailableBudget = availableBudget.merge(reservation);
		if (!totalBudget.isMatching(newAvailableBudget)) {
			return false;
		}

		availableBudget = newAvailableBudget;
		return true;
	}

	private static void checkResourceProfileNotNullOrUnknown(final ResourceProfile resourceProfile) {
		Preconditions.checkNotNull(resourceProfile);
		Preconditions.checkArgument(!resourceProfile.equals(ResourceProfile.UNKNOWN));
	}
}
