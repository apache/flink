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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Supplier;

/**
 * Utilities for factories.
 */
public enum FactoryUtils {
	;

	private static final Logger LOG = LoggerFactory.getLogger(FactoryUtils.class);

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
	public static <R, F> R loadAndInvokeFactory(final Class<F> factoryInterface, final FactoryInvoker<F, R> factoryInvoker, final Supplier<F> defaultProvider) {
		final ServiceLoader<F> factories = ServiceLoader.load(factoryInterface);

		final List<R> instantiatedResources = new ArrayList<>();
		final List<Exception> errorsDuringInitialization = new ArrayList<>();
		for (F factory : factories) {
			try {
				R resource = factoryInvoker.invoke(factory);
				instantiatedResources.add(resource);
				LOG.info("Instantiated {}.", resource.getClass().getSimpleName());
			} catch (Exception e) {
				LOG.debug("Factory {} could not instantiate instance.", factory.getClass().getSimpleName(), e);
				errorsDuringInitialization.add(e);
			}
		}

		if (instantiatedResources.size() == 1) {
			return instantiatedResources.get(0);
		}

		if (instantiatedResources.isEmpty()) {
			try {
				return factoryInvoker.invoke(defaultProvider.get());
			} catch (Exception e) {
				final RuntimeException exception = new RuntimeException("Could not instantiate any instance.");
				final RuntimeException defaultException = new RuntimeException("Could not instantiate default instance.", e);
				exception.addSuppressed(defaultException);
				errorsDuringInitialization.forEach(exception::addSuppressed);
				throw exception;
			}
		}

		throw new RuntimeException("Multiple instances were created: " + instantiatedResources);
	}

	/**
	 * Interface for invoking the factory.
	 */
	public interface FactoryInvoker<F, R> {
		R invoke(F factory) throws Exception;
	}
}
