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

package org.apache.flink.tests.util.util;

import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Utilities for factories.
 */
public enum FactoryUtils {
	;

	/**
	 * Loads all factories for the given class using the {@link ServiceLoader} and attempts to create an instance.
	 *
	 * @param factoryInterface factory interface
	 * @param factoryInvoker factory invoker
	 * @param defaultProvider default factory provider
	 * @param <R> resource type
	 * @param <F> factory type
	 * @throws RuntimeException if no or multiple resources could be instantiated
	 * @return created instance
	 */
	public static <R, F> R loadAndInvokeFactory(final Class<F> factoryInterface, final Function<F, Optional<R>> factoryInvoker, final Supplier<F> defaultProvider) {
		final ServiceLoader<F> factories = ServiceLoader.load(factoryInterface);

		final List<R> resources = StreamSupport.stream(factories.spliterator(), false)
			.map(factoryInvoker)
			.filter(Optional::isPresent)
			.map(Optional::get)
			.collect(Collectors.toList());

		if (resources.size() == 1) {
			return resources.get(0);
		}

		if (resources.isEmpty()) {
			return factoryInvoker.apply(defaultProvider.get())
				.orElseThrow(() -> new RuntimeException("Could not instantiate instance using default factory."));
		}

		throw new RuntimeException("Multiple instances were created: " + resources);
	}
}
