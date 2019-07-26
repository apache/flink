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

package org.apache.flink.table.factories;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.AmbiguousTableFactoryException;
import org.apache.flink.table.api.NoMatchingTableFactoryException;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ExternalCatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.MetadataValidator.METADATA_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.StatisticsValidator.STATISTICS_PROPERTY_VERSION;

/**
 * Unified class to search for a {@link TableFactory} of provided type and properties.
 */
public class TableFactoryService {

	private static final ServiceLoader<TableFactory> defaultLoader = ServiceLoader.load(TableFactory.class);
	private static final Logger LOG = LoggerFactory.getLogger(TableFactoryService.class);

	/**
	 * Finds a table factory of the given class and descriptor.
	 *
	 * @param factoryClass desired factory class
	 * @param descriptor descriptor describing the factory configuration
	 * @param <T> factory class type
	 * @return the matching factory
	 */
	public static <T extends TableFactory> T find(Class<T> factoryClass, Descriptor descriptor) {
		Preconditions.checkNotNull(descriptor);
		return findSingleInternal(factoryClass, descriptor.toProperties(), Optional.empty());
	}

	/**
	 * Finds a table factory of the given class, descriptor, and classloader.
	 *
	 * @param factoryClass desired factory class
	 * @param descriptor descriptor describing the factory configuration
	 * @param classLoader classloader for service loading
	 * @param <T> factory class type
	 * @return the matching factory
	 */
	public static <T extends TableFactory> T find(
			Class<T> factoryClass,
			Descriptor descriptor,
			ClassLoader classLoader) {
		Preconditions.checkNotNull(descriptor);
		Preconditions.checkNotNull(classLoader);
		return findSingleInternal(factoryClass, descriptor.toProperties(), Optional.of(classLoader));
	}

	/**
	 * Finds a table factory of the given class and property map.
	 *
	 * @param factoryClass desired factory class
	 * @param propertyMap properties that describe the factory configuration
	 * @param <T> factory class type
	 * @return the matching factory
	 */
	public static <T extends TableFactory> T find(Class<T> factoryClass, Map<String, String> propertyMap) {
		return findSingleInternal(factoryClass, propertyMap, Optional.empty());
	}

	/**
	 * Finds a table factory of the given class, property map, and classloader.
	 *
	 * @param factoryClass desired factory class
	 * @param propertyMap properties that describe the factory configuration
	 * @param classLoader classloader for service loading
	 * @param <T> factory class type
	 * @return the matching factory
	 */
	public static <T extends TableFactory> T find(
			Class<T> factoryClass,
			Map<String, String> propertyMap,
			ClassLoader classLoader) {
		Preconditions.checkNotNull(classLoader);
		return findSingleInternal(factoryClass, propertyMap, Optional.of(classLoader));
	}

	/**
	 * Finds all table factories of the given class and property map.
	 *
	 * @param factoryClass desired factory class
	 * @param propertyMap properties that describe the factory configuration
	 * @param <T> factory class type
	 * @return all the matching factories
	 */
	public static <T extends TableFactory> List<T> findAll(Class<T> factoryClass, Map<String, String> propertyMap) {
		return findAllInternal(factoryClass, propertyMap, Optional.empty());
	}

	/**
	 * Finds a table factory of the given class, property map, and classloader.
	 *
	 * @param factoryClass desired factory class
	 * @param properties properties that describe the factory configuration
	 * @param classLoader classloader for service loading
	 * @param <T> factory class type
	 * @return the matching factory
	 */
	private static <T extends TableFactory> T findSingleInternal(
			Class<T> factoryClass,
			Map<String, String> properties,
			Optional<ClassLoader> classLoader) {

		List<TableFactory> tableFactories = discoverFactories(classLoader);
		List<T> filtered = filter(tableFactories, factoryClass, properties);

		if (filtered.size() > 1) {
			throw new AmbiguousTableFactoryException(
				filtered,
				factoryClass,
				tableFactories,
				properties);
		} else {
			return filtered.get(0);
		}
	}

	/**
	 * Finds a table factory of the given class, property map, and classloader.
	 *
	 * @param factoryClass desired factory class
	 * @param properties properties that describe the factory configuration
	 * @param classLoader classloader for service loading
	 * @param <T> factory class type
	 * @return the matching factory
	 */
	private static <T extends TableFactory> List<T> findAllInternal(
			Class<T> factoryClass,
			Map<String, String> properties,
			Optional<ClassLoader> classLoader) {

		List<TableFactory> tableFactories = discoverFactories(classLoader);
		return filter(tableFactories, factoryClass, properties);
	}

	/**
	 * Filters found factories by factory class and with matching context.
	 */
	private static <T extends TableFactory> List<T> filter(
			List<TableFactory> foundFactories,
			Class<T> factoryClass,
			Map<String, String> properties) {

		Preconditions.checkNotNull(factoryClass);
		Preconditions.checkNotNull(properties);

		List<T> classFactories = filterByFactoryClass(
			factoryClass,
			properties,
			foundFactories);

		List<T> contextFactories = filterByContext(
			factoryClass,
			properties,
			foundFactories,
			classFactories);

		return filterBySupportedProperties(
			factoryClass,
			properties,
			foundFactories,
			contextFactories);
	}

	/**
	 * Searches for factories using Java service providers.
	 *
	 * @return all factories in the classpath
	 */
	private static List<TableFactory> discoverFactories(Optional<ClassLoader> classLoader) {
		try {
			List<TableFactory> result = new LinkedList<>();
			if (classLoader.isPresent()) {
				ServiceLoader
					.load(TableFactory.class, classLoader.get())
					.iterator()
					.forEachRemaining(result::add);
			} else {
				defaultLoader.iterator().forEachRemaining(result::add);
			}
			return result;
		} catch (ServiceConfigurationError e) {
			LOG.error("Could not load service provider for table factories.", e);
			throw new TableException("Could not load service provider for table factories.", e);
		}

	}

	/**
	 * Filters factories with matching context by factory class.
	 */
	@SuppressWarnings("unchecked")
	private static <T> List<T> filterByFactoryClass(
			Class<T> factoryClass,
			Map<String, String> properties,
			List<TableFactory> foundFactories) {

		List<TableFactory> classFactories = foundFactories.stream()
			.filter(p -> factoryClass.isAssignableFrom(p.getClass()))
			.collect(Collectors.toList());

		if (classFactories.isEmpty()) {
			throw new NoMatchingTableFactoryException(
				String.format("No factory implements '%s'.", factoryClass.getCanonicalName()),
				factoryClass,
				foundFactories,
				properties);
		}

		return (List<T>) classFactories;
	}

	/**
	 * Filters for factories with matching context.
	 *
	 * @return all matching factories
	 */
	private static <T extends TableFactory> List<T> filterByContext(
			Class<T> factoryClass,
			Map<String, String> properties,
			List<TableFactory> foundFactories,
			List<T> classFactories) {

		List<T> matchingFactories = classFactories.stream().filter(factory -> {
			Map<String, String> requestedContext = normalizeContext(factory);

			Map<String, String> plainContext = new HashMap<>(requestedContext);
			// we remove the version for now until we have the first backwards compatibility case
			// with the version we can provide mappings in case the format changes
			plainContext.remove(CONNECTOR_PROPERTY_VERSION);
			plainContext.remove(FORMAT_PROPERTY_VERSION);
			plainContext.remove(METADATA_PROPERTY_VERSION);
			plainContext.remove(STATISTICS_PROPERTY_VERSION);
			plainContext.remove(CATALOG_PROPERTY_VERSION);
			plainContext.remove(org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION);

			// check if required context is met
			return plainContext.keySet()
				.stream()
				.allMatch(e -> properties.containsKey(e) && properties.get(e).equals(plainContext.get(e)));
		}).collect(Collectors.toList());

		if (matchingFactories.isEmpty()) {
			throw new NoMatchingTableFactoryException(
				"No context matches.",
				factoryClass,
				foundFactories,
				properties);
		}

		return matchingFactories;
	}

	/**
	 * Prepares the properties of a context to be used for match operations.
	 */
	private static Map<String, String> normalizeContext(TableFactory factory) {
		Map<String, String> requiredContext = factory.requiredContext();
		if (requiredContext == null) {
			throw new TableException(
				String.format("Required context of factory '%s' must not be null.", factory.getClass().getName()));
		}
		return requiredContext.keySet().stream()
			.collect(Collectors.toMap(String::toLowerCase, requiredContext::get));
	}

	/**
	 * Filters the matching class factories by supported properties.
	 */
	private static <T extends TableFactory> List<T> filterBySupportedProperties(
			Class<T> factoryClass,
			Map<String, String> properties,
			List<TableFactory> foundFactories,
			List<T> classFactories) {

		final List<String> plainGivenKeys = new LinkedList<>();
		properties.keySet().forEach(k -> {
			// replace arrays with wildcard
			String key = k.replaceAll(".\\d+", ".#");
			// ignore duplicates
			if (!plainGivenKeys.contains(key)) {
				plainGivenKeys.add(key);
			}
		});

		Optional<String> lastKey = Optional.empty();
		List<T> supportedFactories = new LinkedList<>();
		for (T factory: classFactories) {
			Set<String> requiredContextKeys = normalizeContext(factory).keySet();
			Tuple2<List<String>, List<String>> tuple2 = normalizeSupportedProperties(factory);
			// ignore context keys
			List<String> givenContextFreeKeys = plainGivenKeys.stream()
				.filter(p -> !requiredContextKeys.contains(p))
				.collect(Collectors.toList());
			List<String> givenFilteredKeys = filterSupportedPropertiesFactorySpecific(
				factory,
				givenContextFreeKeys);

			boolean allTrue = true;
			for (String k: givenFilteredKeys) {
				lastKey = Optional.of(k);
				if (!(tuple2.f0.contains(k) || tuple2.f1.stream().anyMatch(k::startsWith))) {
					allTrue = false;
					break;
				}
			}
			if (allTrue) {
				supportedFactories.add(factory);
			}
		}

		if (supportedFactories.isEmpty() && classFactories.size() == 1 && lastKey.isPresent()) {
			// special case: when there is only one matching factory but the last property key
			// was incorrect
			TableFactory factory = classFactories.get(0);
			Tuple2<List<String>, List<String>> tuple2 = normalizeSupportedProperties(factory);

			String errorMessage = String.format(
				"The matching factory '%s' doesn't support '%s'.\n\nSupported properties of " +
					"this factory are:\n%s",
				factory.getClass().getName(),
				lastKey.get(),
				String.join("\n", tuple2.f0));

			throw new NoMatchingTableFactoryException(
				errorMessage,
				factoryClass,
				foundFactories,
				properties);
		} else if (supportedFactories.isEmpty()) {
			throw new NoMatchingTableFactoryException(
				"No factory supports all properties.",
				factoryClass,
				foundFactories,
				properties);
		}

		return supportedFactories;
	}

	/**
	 * Prepares the supported properties of a factory to be used for match operations.
	 */
	private static Tuple2<List<String>, List<String>> normalizeSupportedProperties(TableFactory factory) {
		List<String> supportedProperties = factory.supportedProperties();
		if (supportedProperties == null) {
			throw new TableException(
				String.format("Supported properties of factory '%s' must not be null.",
					factory.getClass().getName()));
		}
		List<String> supportedKeys = supportedProperties.stream()
			.map(String::toLowerCase)
			.collect(Collectors.toList());

		// extract wildcard prefixes
		List<String> wildcards = extractWildcardPrefixes(supportedKeys);
		return Tuple2.of(supportedKeys, wildcards);
	}

	/**
	 * Converts the prefix of properties with wildcards (e.g., "format.*").
	 */
	private static List<String> extractWildcardPrefixes(List<String> propertyKeys) {
		return propertyKeys.stream()
			.filter(p -> p.endsWith("*"))
			.map(s -> s.substring(0, s.length() - 1))
			.collect(Collectors.toList());
	}

	/**
	 * Performs filtering for special cases (i.e. table format factories with schema derivation).
	 */
	private static List<String> filterSupportedPropertiesFactorySpecific(TableFactory factory, List<String> keys) {

		if (factory instanceof TableFormatFactory) {
			boolean includeSchema = ((TableFormatFactory) factory).supportsSchemaDerivation();
			return keys.stream().filter(k -> {
				if (includeSchema) {
					return k.startsWith(Schema.SCHEMA + ".") ||
						k.startsWith(FormatDescriptorValidator.FORMAT + ".");
				} else {
					return k.startsWith(FormatDescriptorValidator.FORMAT + ".");
				}
			}).collect(Collectors.toList());
		} else {
			return keys;
		}
	}
}
