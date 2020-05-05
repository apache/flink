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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.ScanFormat;
import org.apache.flink.table.connector.format.SinkFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.utils.EncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility for working with {@link Factory}s.
 */
@PublicEvolving
public final class FactoryUtil {

	private static final Logger LOG = LoggerFactory.getLogger(FactoryUtil.class);

	public static final ConfigOption<Integer> PROPERTY_VERSION = ConfigOptions.key("property-version")
		.intType()
		.defaultValue(1)
		.withDescription(
			"Version of the overall property design. This option is meant for future backwards compatibility.");

	public static final ConfigOption<String> CONNECTOR = ConfigOptions.key("connector")
		.stringType()
		.noDefaultValue()
		.withDescription(
			"Uniquely identifies the connector of a dynamic table that is used for accessing data in " +
			"an external system. Its value is used during table source and table sink discovery.");

	public static final String FORMAT_PREFIX = "format.";

	public static final String KEY_FORMAT_PREFIX = "key.format.";

	public static final String VALUE_FORMAT_PREFIX = "value.format.";

	/**
	 * Creates a {@link DynamicTableSource} from a {@link CatalogTable}.
	 *
	 * <p>It considers {@link Catalog#getFactory()} if provided.
	 */
	public static DynamicTableSource createTableSource(
			@Nullable Catalog catalog,
			ObjectIdentifier objectIdentifier,
			CatalogTable catalogTable,
			ReadableConfig configuration,
			ClassLoader classLoader) {
		final DefaultDynamicTableContext context = new DefaultDynamicTableContext(
			objectIdentifier,
			catalogTable,
			configuration,
			classLoader);
		try {
			final DynamicTableSourceFactory factory = getDynamicTableFactory(
				DynamicTableSourceFactory.class,
				catalog,
				context);
			return factory.createDynamicTableSource(context);
		} catch (Throwable t) {
			throw new ValidationException(
				String.format(
					"Unable to create a source for reading table '%s'.\n\n" +
					"Table options are:\n\n" +
					"%s",
					objectIdentifier.asSummaryString(),
					catalogTable.getOptions()
						.entrySet()
						.stream()
						.map(e -> stringifyOption(e.getKey(), e.getValue()))
						.sorted()
						.collect(Collectors.joining("\n"))),
				t);
		}
	}

	/**
	 * Creates a {@link DynamicTableSink} from a {@link CatalogTable}.
	 *
	 * <p>It considers {@link Catalog#getFactory()} if provided.
	 */
	public static DynamicTableSink createTableSink(
			@Nullable Catalog catalog,
			ObjectIdentifier objectIdentifier,
			CatalogTable catalogTable,
			ReadableConfig configuration,
			ClassLoader classLoader) {
		final DefaultDynamicTableContext context = new DefaultDynamicTableContext(
			objectIdentifier,
			catalogTable,
			configuration,
			classLoader);
		try {
			final DynamicTableSinkFactory factory = getDynamicTableFactory(
				DynamicTableSinkFactory.class,
				catalog,
				context);
			return factory.createDynamicTableSink(context);
		} catch (Throwable t) {
			throw new ValidationException(
				String.format(
					"Unable to create a sink for writing table '%s'.\n\n" +
					"Table options are:\n\n" +
					"%s",
					objectIdentifier.asSummaryString(),
					catalogTable.getOptions()
						.entrySet()
						.stream()
						.map(e -> stringifyOption(e.getKey(), e.getValue()))
						.sorted()
						.collect(Collectors.joining("\n"))),
				t);
		}
	}

	/**
	 * Creates a utility that helps in discovering formats and validating all options for a {@link DynamicTableFactory}.
	 *
	 * <p>The following example sketches the usage:
	 * <pre>{@code
	 * // in createDynamicTableSource()
	 * helper = FactoryUtil.createTableFactoryHelper(this, context);
	 * keyFormat = helper.discoverScanFormat(classloader, MyFormatFactory.class, KEY_OPTION, "prefix");
	 * valueFormat = helper.discoverScanFormat(classloader, MyFormatFactory.class, VALUE_OPTION, "prefix");
	 * helper.validate();
	 * ... // construct connector with discovered formats
	 * }</pre>
	 *
	 * <p>Note: This utility checks for left-over options in the final step.
	 */
	public static TableFactoryHelper createTableFactoryHelper(
			DynamicTableFactory factory,
			DynamicTableFactory.Context context) {
		return new TableFactoryHelper(factory, context);
	}

	/**
	 * Discovers a factory using the given factory base class and identifier.
	 *
	 * <p>This method is meant for cases where {@link #createTableFactoryHelper(DynamicTableFactory, DynamicTableFactory.Context)}
	 * {@link #createTableSource(Catalog, ObjectIdentifier, CatalogTable, ReadableConfig, ClassLoader)},
	 * and {@link #createTableSink(Catalog, ObjectIdentifier, CatalogTable, ReadableConfig, ClassLoader)}
	 * are not applicable.
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Factory> T discoverFactory(
			ClassLoader classLoader,
			Class<T> factoryClass,
			String factoryIdentifier) {
		final List<Factory> factories = discoverFactories(classLoader);

		final List<Factory> foundFactories = factories.stream()
			.filter(f -> factoryClass.isAssignableFrom(f.getClass()))
			.collect(Collectors.toList());

		if (foundFactories.isEmpty()) {
			throw new ValidationException(
				String.format(
					"Could not find any factories that implement '%s' in the classpath.",
					factoryClass.getName()));
		}

		final List<Factory> matchingFactories = foundFactories.stream()
			.filter(f -> f.factoryIdentifier().equals(factoryIdentifier))
			.collect(Collectors.toList());

		if (matchingFactories.isEmpty()) {
			throw new ValidationException(
				String.format(
					"Could not find any factory for identifier '%s' that implements '%s' in the classpath.\n\n" +
					"Available factory identifiers are:\n\n" +
					"%s",
					factoryIdentifier,
					factoryClass.getName(),
					foundFactories.stream()
						.map(Factory::factoryIdentifier)
						.sorted()
						.collect(Collectors.joining("\n"))));
		}
		if (matchingFactories.size() > 1) {
			throw new ValidationException(
				String.format(
					"Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n" +
					"Ambiguous factory classes are:\n\n" +
					"%s",
					factoryIdentifier,
					factoryClass.getName(),
					foundFactories.stream()
						.map(f -> factories.getClass().getName())
						.sorted()
						.collect(Collectors.joining("\n"))));
		}

		return (T) matchingFactories.get(0);
	}

	/**
	 * Validates the required and optional {@link ConfigOption}s of a factory.
	 *
	 * <p>Note: It does not check for left-over options.
	 */
	public static void validateFactoryOptions(Factory factory, ReadableConfig options) {
		// currently Flink's options have no validation feature which is why we access them eagerly
		// to provoke a parsing error

		final List<String> missingRequiredOptions = factory.requiredOptions().stream()
			.filter(option -> readOption(options, option) == null)
			.map(ConfigOption::key)
			.sorted()
			.collect(Collectors.toList());

		if (!missingRequiredOptions.isEmpty()) {
			throw new ValidationException(
				String.format(
					"One or more required options are missing.\n\n" +
					"Missing required options are:\n\n" +
					"%s",
					String.join("\n", missingRequiredOptions)));
		}

		factory.optionalOptions()
			.forEach(option -> readOption(options, option));
	}

	// --------------------------------------------------------------------------------------------
	// Helper methods
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private static <T extends DynamicTableFactory> T getDynamicTableFactory(
			Class<T> factoryClass,
			@Nullable Catalog catalog,
			DefaultDynamicTableContext context) {
		// catalog factory has highest precedence
		if (catalog != null) {
			final Factory factory = catalog.getFactory()
				.filter(f -> factoryClass.isAssignableFrom(f.getClass()))
				.orElse(null);
			if (factory != null) {
				return (T) factory;
			}
		}

		// fallback to factory discovery
		final String connectorOption = context.getCatalogTable()
			.getOptions()
			.get(CONNECTOR.key());
		if (connectorOption == null) {
			throw new ValidationException(
				String.format(
					"Table options do not contain an option key '%s' for discovering a connector.",
					CONNECTOR.key()));
		}
		try {
			return discoverFactory(context.getClassLoader(), factoryClass, connectorOption);
		} catch (ValidationException e) {
			throw new ValidationException(
				String.format(
					"Cannot discover a connector using option '%s'.",
					stringifyOption(CONNECTOR.key(), connectorOption)),
				e);
		}
	}

	private static List<Factory> discoverFactories(ClassLoader classLoader) {
		try {
			final List<Factory> result = new LinkedList<>();
			ServiceLoader
				.load(Factory.class, classLoader)
				.iterator()
				.forEachRemaining(result::add);
			return result;
		} catch (ServiceConfigurationError e) {
			LOG.error("Could not load service provider for factories.", e);
			throw new TableException("Could not load service provider for factories.", e);
		}
	}

	private static String stringifyOption(String key, String value) {
		return String.format(
			"'%s'='%s'",
			EncodingUtils.escapeSingleQuotes(key),
			EncodingUtils.escapeSingleQuotes(value));
	}

	private static Configuration asConfiguration(Map<String, String> options) {
		final Configuration configuration = new Configuration();
		options.forEach(configuration::setString);
		return configuration;
	}

	private static <T> T readOption(ReadableConfig options, ConfigOption<T> option) {
		try {
			return options.get(option);
		} catch (Throwable t) {
			throw new ValidationException(String.format("Invalid value for option '%s'.", option.key()), t);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Helper classes
	// --------------------------------------------------------------------------------------------

	/**
	 * Helper utility for discovering formats and validating all options for a {@link DynamicTableFactory}.
	 *
	 * @see #createTableFactoryHelper(DynamicTableFactory, DynamicTableFactory.Context)
	 */
	public static class TableFactoryHelper {

		private final DynamicTableFactory tableFactory;

		private final DynamicTableFactory.Context context;

		private final Configuration allOptions;

		private final Set<String> consumedOptionKeys;

		private TableFactoryHelper(DynamicTableFactory tableFactory, DynamicTableFactory.Context context) {
			this.tableFactory = tableFactory;
			this.context = context;
			this.allOptions = asConfiguration(context.getCatalogTable().getOptions());
			this.consumedOptionKeys = new HashSet<>();
			this.consumedOptionKeys.add(PROPERTY_VERSION.key());
			this.consumedOptionKeys.add(CONNECTOR.key());
			this.consumedOptionKeys.addAll(
				tableFactory.requiredOptions().stream()
					.map(ConfigOption::key)
					.collect(Collectors.toSet()));
			this.consumedOptionKeys.addAll(
				tableFactory.optionalOptions().stream()
					.map(ConfigOption::key)
					.collect(Collectors.toSet()));
		}

		/**
		 * Discovers a {@link ScanFormat} of the given type using the given option as factory identifier.
		 *
		 * <p>A prefix, e.g. {@link #KEY_FORMAT_PREFIX}, projects the options for the format factory.
		 */
		public <I, F extends ScanFormatFactory<I>> ScanFormat<I> discoverScanFormat(
				Class<F> formatFactoryClass,
				ConfigOption<String> formatOption,
				String formatPrefix) {
			return discoverOptionalScanFormat(formatFactoryClass, formatOption, formatPrefix)
				.orElseThrow(() ->
					new ValidationException(
						String.format("Could not find required scan format '%s'.", formatOption.key())));
		}

		/**
		 * Discovers a {@link ScanFormat} of the given type using the given option (if present) as factory
		 * identifier.
		 *
		 * <p>A prefix, e.g. {@link #KEY_FORMAT_PREFIX}, projects the options for the format factory.
		 */
		public <I, F extends ScanFormatFactory<I>> Optional<ScanFormat<I>> discoverOptionalScanFormat(
				Class<F> formatFactoryClass,
				ConfigOption<String> formatOption,
				String formatPrefix) {
			return discoverOptionalFormatFactory(formatFactoryClass, formatOption, formatPrefix)
				.map(formatFactory -> {
					try {
						return formatFactory.createScanFormat(context, projectOptions(formatPrefix));
					} catch (Throwable t) {
						throw new ValidationException(
							String.format(
								"Error creating scan format '%s' in option space '%s'.",
								formatFactory.factoryIdentifier(),
								formatPrefix),
							t);
					}
				});
		}

		/**
		 * Discovers a {@link SinkFormat} of the given type using the given option as factory identifier.
		 *
		 * <p>A prefix, e.g. {@link #KEY_FORMAT_PREFIX}, projects the options for the format factory.
		 */
		public <I, F extends SinkFormatFactory<I>> SinkFormat<I> discoverSinkFormat(
				Class<F> formatFactoryClass,
				ConfigOption<String> formatOption,
				String formatPrefix) {
			return discoverOptionalSinkFormat(formatFactoryClass, formatOption, formatPrefix)
				.orElseThrow(() ->
					new ValidationException(
						String.format("Could not find required sink format '%s'.", formatOption.key())));
		}

		/**
		 * Discovers a {@link SinkFormat} of the given type using the given option (if present) as factory
		 * identifier.
		 *
		 * <p>A prefix, e.g. {@link #KEY_FORMAT_PREFIX}, projects the options for the format factory.
		 */
		public <I, F extends SinkFormatFactory<I>> Optional<SinkFormat<I>> discoverOptionalSinkFormat(
				Class<F> formatFactoryClass,
				ConfigOption<String> formatOption,
				String formatPrefix) {
			return discoverOptionalFormatFactory(formatFactoryClass, formatOption, formatPrefix)
				.map(formatFactory -> {
					try {
						return formatFactory.createSinkFormat(context, projectOptions(formatPrefix));
					} catch (Throwable t) {
						throw new ValidationException(
							String.format(
								"Error creating sink format '%s' in option space '%s'.",
								formatFactory.factoryIdentifier(),
								formatPrefix),
							t);
					}
				});
		}

		/**
		 * Validates the options of the {@link DynamicTableFactory}. It checks for unconsumed option
		 * keys.
		 */
		public void validate() {
			validateFactoryOptions(tableFactory, allOptions);
			final Set<String> remainingOptionKeys = new HashSet<>(allOptions.keySet());
			remainingOptionKeys.removeAll(consumedOptionKeys);
			if (remainingOptionKeys.size() > 0) {
				throw new ValidationException(
					String.format(
						"Unsupported options found for connector '%s'.\n\n" +
						"Unsupported options:\n\n" +
						"%s\n\n" +
						"Supported options:\n\n" +
						"%s",
						tableFactory.factoryIdentifier(),
						remainingOptionKeys.stream()
							.sorted()
							.collect(Collectors.joining("\n")),
						consumedOptionKeys.stream()
							.sorted()
							.collect(Collectors.joining("\n"))));
			}
		}

		/**
		 * Returns all options of the table.
		 */
		public ReadableConfig getOptions() {
			return allOptions;
		}

		// ----------------------------------------------------------------------------------------

		private <F extends Factory> Optional<F> discoverOptionalFormatFactory(
				Class<F> formatFactoryClass,
				ConfigOption<String> formatOption,
				String formatPrefix) {
			final String identifier = allOptions.get(formatOption);
			if (identifier == null) {
				return Optional.empty();
			}
			final F factory = discoverFactory(
				context.getClassLoader(),
				formatFactoryClass,
				identifier);
			// log all used options of other factories
			consumedOptionKeys.addAll(
				factory.requiredOptions().stream()
					.map(ConfigOption::key)
					.map(k -> formatPrefix + k)
					.collect(Collectors.toSet()));
			consumedOptionKeys.addAll(
				factory.optionalOptions().stream()
					.map(ConfigOption::key)
					.map(k -> formatPrefix + k)
					.collect(Collectors.toSet()));
			return Optional.of(factory);
		}

		private ReadableConfig projectOptions(String formatPrefix) {
			return new DelegatingConfiguration(
				allOptions,
				formatPrefix);
		}
	}

	private static class DefaultDynamicTableContext implements DynamicTableFactory.Context {

		private final ObjectIdentifier objectIdentifier;
		private final CatalogTable catalogTable;
		private final ReadableConfig configuration;
		private final ClassLoader classLoader;

		DefaultDynamicTableContext(
				ObjectIdentifier objectIdentifier,
				CatalogTable catalogTable,
				ReadableConfig configuration,
				ClassLoader classLoader) {
			this.objectIdentifier = objectIdentifier;
			this.catalogTable = catalogTable;
			this.configuration = configuration;
			this.classLoader = classLoader;
		}

		@Override
		public ObjectIdentifier getObjectIdentifier() {
			return objectIdentifier;
		}

		@Override
		public CatalogTable getCatalogTable() {
			return catalogTable;
		}

		@Override
		public ReadableConfig getConfiguration() {
			return configuration;
		}

		@Override
		public ClassLoader getClassLoader() {
			return classLoader;
		}
	}

	// --------------------------------------------------------------------------------------------

	private FactoryUtil() {
		// no instantiation
	}
}
