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

package org.apache.flink.ml.api.misc.factory;

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract factory loader to search and load a specific BaseFactory with class and properties.
 *
 * @param <T> type of desired base factory class, subclasses for loading should be registered in the
 *            file META-INF/services/{@code T.class.getName()}
 */
@PublicEvolving
public abstract class BaseFactoryLoader<T extends BaseFactory> {
	private final ServiceLoader<T> defaultLoader = ServiceLoader.load(getFactoryClass());

	/**
	 * Finds a factory of the given class and property map, using the current thread's {@linkplain
	 * java.lang.Thread#getContextClassLoader context class loader}.
	 *
	 * @param <ST>         type of desired factory class, which is a subclass of the class returned
	 *                     by getFactoryClass()
	 * @param factoryClass desired factory class
	 * @param propertyMap  properties that describe the factory configuration
	 * @return the matching factory
	 */
	public <ST extends T> ST find(Class<ST> factoryClass, Map<String, String> propertyMap) {
		return find(factoryClass, propertyMap, null);
	}

	/**
	 * Finds a factory of the given class and property map using the given classloader.
	 *
	 * @param <ST>         type of desired factory class, which is a subclass of the class returned
	 *                     by getFactoryClass()
	 * @param factoryClass desired factory class
	 * @param propertyMap  properties that describe the factory configuration
	 * @param classLoader  classloader for factory loading, use the current thread's {@linkplain
	 *                     java.lang.Thread#getContextClassLoader context class loader} if is null
	 * @return the matching factory
	 */
	public <ST extends T> ST find(
		Class<ST> factoryClass,
		Map<String, String> propertyMap,
		@Nullable ClassLoader classLoader) {

		return findInternal(factoryClass, propertyMap, classLoader);
	}

	/**
	 * Returns the factory class this factory loader is used to load. All factories found with find
	 * methods are instances of this class or its subclasses.
	 *
	 * @return the factory class this factory loader is used to load.
	 */
	protected abstract Class<T> getFactoryClass();

	/**
	 * Validate whether a factory matches customized requirements. All factories match by default.
	 * Implementations of BaseFactoryLoader may override this method for customized matching.
	 *
	 * @param factory    factory instance to validate
	 * @param propertyMap properties that describe the factory configuration
	 * @return whether the factory matches customized requirements, always true if not overridden
	 */
	protected boolean matchCustomizedRequirements(T factory, Map<String, String> propertyMap) {
		return true;
	}

	@SuppressWarnings("unchecked")
	private <ST extends T> ST findInternal(
		Class<ST> factoryClass,
		Map<String, String> properties,
		@Nullable ClassLoader classLoader) {

		List<T> foundFactorys = discoverFactorys(classLoader);

		List<ST> factoryClassFactorys = foundFactorys.stream()
			.filter(f -> matchClass(f, factoryClass))
			.map(f -> (ST) f)
			.collect(Collectors.toList());

		List<ST> matchFactorys = factoryClassFactorys.stream()
			.filter(f -> matchRequiredContext(f, properties))
			.filter(f -> matchRequiredProperties(f, properties))
			.filter(f -> matchSupportedProperties(f, properties))
			.filter(f -> matchCustomizedRequirements(f, properties))
			.collect(Collectors.toList());

		return getTheOnlyMatchingFactory(matchFactorys, properties, foundFactorys);
	}

	private List<T> discoverFactorys(@Nullable ClassLoader classLoader) {
		try {
			List<T> result = new LinkedList<>();
			ServiceLoader<T> loader = defaultLoader;
			if (classLoader != null) {
				loader = ServiceLoader.load(getFactoryClass(), classLoader);
			}
			loader.iterator().forEachRemaining(result::add);
			return result;
		} catch (ServiceConfigurationError e) {
			throw new RuntimeException(
				"Could not load factory provider for " + getFactoryClass().getTypeName(), e);
		}
	}

	private <ST extends T> boolean matchClass(T factory, Class<ST> factoryClass) {
		return factoryClass.isAssignableFrom(factory.getClass());
	}

	private boolean matchRequiredContext(T factory, Map<String, String> properties) {
		for (Map.Entry<String, String> p : factory.requiredContext().entrySet()) {
			String name = p.getKey();
			String value = p.getValue();
			if (properties.containsKey(name) && value.equals(properties.get(name))) {
				continue;
			}
			return false;
		}
		return true;
	}

	private boolean matchRequiredProperties(T factory, Map<String, String> properties) {
		return factory.requiredProperties().stream().filter(properties::containsKey).count() ==
			factory.requiredProperties().size();
	}

	private boolean matchSupportedProperties(T factory, Map<String, String> properties) {
		Set<String> supportedProperties = new HashSet<>();
		supportedProperties.addAll(factory.requiredContext().keySet());
		supportedProperties.addAll(factory.requiredProperties());
		supportedProperties.addAll(factory.supportedProperties());

		return properties.keySet().stream().filter(supportedProperties::contains).count() ==
			properties.size();
	}

	private <S> S getTheOnlyMatchingFactory(
		List<S> matchFactorys,
		Map<String, String> properties,
		List<?> foundFactorys) {

		if (matchFactorys.size() > 1) {
			throw new RuntimeException(String.format(
				"More than one factories match, propertyMap is:\n%s,\n matched factories are:\n%s",
				properties.toString(),
				Arrays.toString(matchFactorys.stream()
					.map(s -> s.getClass().getName())
					.toArray(String[]::new))
			));
		}
		if (matchFactorys.isEmpty()) {
			throw new RuntimeException(String.format(
				"No factory matches, propertyMap is:\n%s,\n all found factories are:\n%s",
				properties.toString(),
				Arrays.toString(foundFactorys.stream()
					.map(s -> s.getClass().getName())
					.toArray(String[]::new))
			));
		}
		return matchFactorys.get(0);
	}
}
