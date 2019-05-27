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

package org.apache.flink.runtime.rest.compatibility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The (potentially aggregated) result of a compatibility check that may also contain a list of {@link AssertionError}
 * for each found incompatibility.
 */
final class CompatibilityCheckResult {

	private final Compatibility backwardCompatibility;
	private final int backwardCompatibilityGrade;
	private final Collection<AssertionError> backwardCompatibilityErrors;

	private CompatibilityCheckResult(final Compatibility backwardCompatibility) {
		this(backwardCompatibility, 1, Collections.emptyList());
		if (backwardCompatibility == Compatibility.INCOMPATIBLE) {
			throw new RuntimeException("This constructor must not be used for incompatible results.");
		}
	}

	private CompatibilityCheckResult(final Compatibility backwardCompatibility, final int backwardCompatibilityGrade, final Collection<AssertionError> backwardCompatibilityErrors) {
		this.backwardCompatibility = backwardCompatibility;
		this.backwardCompatibilityGrade = backwardCompatibilityGrade;
		this.backwardCompatibilityErrors = Collections.unmodifiableCollection(backwardCompatibilityErrors);
	}

	public Compatibility getBackwardCompatibility() {
		return backwardCompatibility;
	}

	public int getBackwardCompatibilityGrade() {
		return backwardCompatibilityGrade;
	}

	public Collection<AssertionError> getBackwardCompatibilityErrors() {
		return backwardCompatibilityErrors;
	}

	CompatibilityCheckResult merge(final CompatibilityCheckResult other) {
		final Compatibility mergedCompatibility = this.backwardCompatibility.merge(other.backwardCompatibility);

		final int mergedGrade = this.backwardCompatibilityGrade + other.backwardCompatibilityGrade;

		final List<AssertionError> mergedErrors = new ArrayList<>(this.backwardCompatibilityErrors.size() + other.backwardCompatibilityErrors.size());
		mergedErrors.addAll(this.backwardCompatibilityErrors);
		mergedErrors.addAll(other.backwardCompatibilityErrors);

		return new CompatibilityCheckResult(mergedCompatibility, mergedGrade, mergedErrors);
	}

	public static CompatibilityCheckResult identical() {
		return new CompatibilityCheckResult(Compatibility.IDENTICAL);
	}

	public static CompatibilityCheckResult compatible() {
		return new CompatibilityCheckResult(Compatibility.COMPATIBLE);
	}

	public static CompatibilityCheckResult incompatible(final AssertionError backwardCompatibilityError) {
		return new CompatibilityCheckResult(Compatibility.INCOMPATIBLE, 0, Collections.singletonList(backwardCompatibilityError));
	}
}
