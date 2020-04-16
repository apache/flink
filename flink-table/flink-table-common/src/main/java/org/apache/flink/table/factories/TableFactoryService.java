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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_PROPERTY_VERSION;

/**
 * Unified class to search for a {@link TableFactory} of provided type and properties.
 */
public class TableFactoryService {

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
			classFactories);

		return filterBySupportedProperties(
			factoryClass,
			properties,
			classFactories,
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
			ClassLoader cl = classLoader.orElse(Thread.currentThread().getContextClassLoader());
			ServiceLoader
				.load(TableFactory.class, cl)
				.iterator()
				.forEachRemaining(result::add);
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
			List<T> classFactories) {

		List<T> matchingFactories = new ArrayList<>();
		ContextBestMatched<T> bestMatched = null;
		for (T factory : classFactories) {
			Map<String, String> requestedContext = normalizeContext(factory);

			Map<String, String> plainContext = new HashMap<>(requestedContext);
			// we remove the version for now until we have the first backwards compatibility case
			// with the version we can provide mappings in case the format changes
			plainContext.remove(CONNECTOR_PROPERTY_VERSION);
			plainContext.remove(FORMAT_PROPERTY_VERSION);
			plainContext.remove(CATALOG_PROPERTY_VERSION);

			// check if required context is met
			Map<String, Tuple2<String, String>> mismatchedProperties = new HashMap<>();
			Map<String, String> missingProperties = new HashMap<>();
			for (Map.Entry<String, String> e : plainContext.entrySet()) {
				if (properties.containsKey(e.getKey())) {
					String fromProperties = properties.get(e.getKey());
					if (!Objects.equals(fromProperties, e.getValue())) {
						mismatchedProperties.put(e.getKey(), new Tuple2<>(e.getValue(), fromProperties));
					}
				} else {
					missingProperties.put(e.getKey(), e.getValue());
				}
			}
			int matchedSize = plainContext.size() - mismatchedProperties.size() - missingProperties.size();
			if (matchedSize == plainContext.size()) {
				matchingFactories.add(factory);
			} else {
				if (bestMatched == null || matchedSize > bestMatched.matchedSize) {
					bestMatched = new ContextBestMatched<>(
							factory, matchedSize, mismatchedProperties, missingProperties);
				}
			}
		}

		if (matchingFactories.isEmpty()) {
			String bestMatchedMessage = null;
			if (bestMatched != null && bestMatched.matchedSize > 0) {
				StringBuilder builder = new StringBuilder();
				builder.append(bestMatched.factory.getClass().getName());

				if (bestMatched.missingProperties.size() > 0) {
					builder.append("\nMissing properties:");
					bestMatched.missingProperties.forEach((k, v) ->
							builder.append("\n").append(k).append("=").append(v));
				}

				if (bestMatched.mismatchedProperties.size() > 0) {
					builder.append("\nMismatched properties:");
					bestMatched.mismatchedProperties
						.entrySet()
						.stream()
						.filter(e -> e.getValue().f1 != null)
						.forEach(e -> builder.append(
							String.format(
								"\n'%s' expects '%s', but is '%s'",
								e.getKey(),
								e.getValue().f0,
								e.getValue().f1)));
				}

				bestMatchedMessage = builder.toString();
			}
			//noinspection unchecked
			throw new NoMatchingTableFactoryException(
				"Required context properties mismatch.",
				bestMatchedMessage,
				factoryClass,
				(List<TableFactory>) classFactories,
				properties);
		}

		return matchingFactories;
	}

	private static class ContextBestMatched<T extends TableFactory> {

		private final T factory;

		private final int matchedSize;

		/**
		 * Key -> (value in factory, value in properties).
		 */
		private final Map<String, Tuple2<String, String>> mismatchedProperties;

		/**
		 * Key -> value in factory.
		 */
		private final Map<String, String> missingProperties;

		private ContextBestMatched(
				T factory,
				int matchedSize,
				Map<String, Tuple2<String, String>> mismatchedProperties,
				Map<String, String> missingProperties) {
			this.factory = factory;
			this.matchedSize = matchedSize;
			this.mismatchedProperties = mismatchedProperties;
			this.missingProperties = missingProperties;
		}
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
			List<T> classFactories,
			List<T> contextFactories) {

		final List<String> plainGivenKeys = new LinkedList<>();
		properties.keySet().forEach(k -> {
			// replace arrays with wildcard
			String key = k.replaceAll(".\\d+", ".#");
			// ignore duplicates
			if (!plainGivenKeys.contains(key)) {
				plainGivenKeys.add(key);
			}
		});

		List<T> supportedFactories = new LinkedList<>();
		Tuple2<T, List<String>> bestMatched = null;
		for (T factory: contextFactories) {
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
			List<String> unsupportedKeys = new ArrayList<>();
			for (String k : givenFilteredKeys) {
				if (!(tuple2.f0.contains(k) || tuple2.f1.stream().anyMatch(k::startsWith))) {
					allTrue = false;
					unsupportedKeys.add(k);
				}
			}
			if (allTrue) {
				supportedFactories.add(factory);
			} else {
				if (bestMatched == null || unsupportedKeys.size() < bestMatched.f1.size()) {
					bestMatched = new Tuple2<>(factory, unsupportedKeys);
				}
			}
		}

		if (supportedFactories.isEmpty()) {
			String bestMatchedMessage = null;
			if (bestMatched != null) {
				bestMatchedMessage = String.format(
						"%s\nUnsupported property keys:\n%s",
						bestMatched.f0.getClass().getName(),
						String.join("\n", bestMatched.f1)
				);
			}

			//noinspection unchecked
			throw new NoMatchingTableFactoryException(
				"No factory supports all properties.",
				bestMatchedMessage,
				factoryClass,
				(List<TableFactory>) classFactories,
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
