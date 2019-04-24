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

import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Routine for checking the compatibility of a {@link MessageHeaders} pair.
 *
 * <p>The 'extractor' {@link Function} generates a 'container', a jackson-compatible object containing the data that
 * the routine bases it's compatibility-evaluation on.
 * The 'assertion' {@link BiConsumer} accepts a pair of containers and asserts the compatibility. Incompatibilities are
 * signaled by throwing an {@link AssertionError}. This implies that the method body will typically contain jUnit
 * assertions.
 */
final class CompatibilityRoutine<C> {

	private final String key;
	private final Class<C> containerClass;
	private final Function<MessageHeaders<?, ?, ?>, C> extractor;
	private final BiConsumer<C, C> assertion;

	CompatibilityRoutine(
			final String key,
			final Class<C> containerClass,
			final Function<MessageHeaders<?, ?, ?>, C> extractor,
			final BiConsumer<C, C> assertion) {
		this.key = key;
		this.containerClass = containerClass;
		this.extractor = extractor;
		this.assertion = assertion;
	}

	String getKey() {
		return key;
	}

	Class<C> getContainerClass() {
		return containerClass;
	}

	C getContainer(final MessageHeaders<?, ?, ?> header) {
		final C container = extractor.apply(header);
		Assert.assertNotNull("Implementation error: Extractor returned null.", container);
		return container;
	}

	CompatibilityCheckResult checkCompatibility(final Optional<C> old, final Optional<C> cur) {
		Preconditions.checkArgument(
			old.isPresent() || cur.isPresent(),
			"Implementation error: Compatibility check container for routine %s for both old and new version is null.", key);

		if (!old.isPresent()) {
			// allow addition of new compatibility routines
			return CompatibilityCheckResult.compatible();
		}
		if (!cur.isPresent()) {
			// forbid removal of compatibility routines
			return CompatibilityCheckResult.incompatible(
				new AssertionError(String.format(
					"Compatibility check container for routine %s not found in current version.", key)));
		}

		try {
			assertion.accept(old.get(), cur.get());
		} catch (final AssertionError e) {
			final AssertionError backwardIncompatibilityCause = new AssertionError(key + ": " + e.getMessage());
			return CompatibilityCheckResult.incompatible(backwardIncompatibilityCause);
		}

		try {
			assertion.accept(cur.get(), old.get());
			return CompatibilityCheckResult.identical();
		} catch (final AssertionError e) {
			return CompatibilityCheckResult.compatible();
		}
	}
}
