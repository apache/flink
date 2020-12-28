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
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/** Utility for working with {@link Factory}s. */
@PublicEvolving
public final class FactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FactoryUtil.class);

    public static final ConfigOption<Integer> PROPERTY_VERSION =
            ConfigOptions.key("property-version")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Version of the overall property design. This option is meant for future backwards compatibility.");

    public static final ConfigOption<String> CONNECTOR =
            ConfigOptions.key("connector")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Uniquely identifies the connector of a dynamic table that is used for accessing data in "
                                    + "an external system. Its value is used during table source and table sink discovery.");

    public static final ConfigOption<String> FORMAT =
            ConfigOptions.key("format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a custom parallelism for the sink. "
                                    + "By default, if this option is not defined, the planner will derive the parallelism "
                                    + "for each statement individually by also considering the global configuration.");

    /**
     * Suffix for keys of {@link ConfigOption} in case a connector requires multiple formats (e.g.
     * for both key and value).
     *
     * <p>See {@link #createTableFactoryHelper(DynamicTableFactory, DynamicTableFactory.Context)}
     * for more information.
     */
    public static final String FORMAT_SUFFIX = ".format";

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
            ClassLoader classLoader,
            boolean isTemporary) {
        final DefaultDynamicTableContext context =
                new DefaultDynamicTableContext(
                        objectIdentifier, catalogTable, configuration, classLoader, isTemporary);
        try {
            final DynamicTableSourceFactory factory =
                    getDynamicTableFactory(DynamicTableSourceFactory.class, catalog, context);
            return factory.createDynamicTableSource(context);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format(
                            "Unable to create a source for reading table '%s'.\n\n"
                                    + "Table options are:\n\n"
                                    + "%s",
                            objectIdentifier.asSummaryString(),
                            catalogTable.getOptions().entrySet().stream()
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
            ClassLoader classLoader,
            boolean isTemporary) {
        final DefaultDynamicTableContext context =
                new DefaultDynamicTableContext(
                        objectIdentifier, catalogTable, configuration, classLoader, isTemporary);
        try {
            final DynamicTableSinkFactory factory =
                    getDynamicTableFactory(DynamicTableSinkFactory.class, catalog, context);
            return factory.createDynamicTableSink(context);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format(
                            "Unable to create a sink for writing table '%s'.\n\n"
                                    + "Table options are:\n\n"
                                    + "%s",
                            objectIdentifier.asSummaryString(),
                            catalogTable.getOptions().entrySet().stream()
                                    .map(e -> stringifyOption(e.getKey(), e.getValue()))
                                    .sorted()
                                    .collect(Collectors.joining("\n"))),
                    t);
        }
    }

    /**
     * Creates a utility that helps in discovering formats and validating all options for a {@link
     * DynamicTableFactory}.
     *
     * <p>The following example sketches the usage:
     *
     * <pre>{@code
     * // in createDynamicTableSource()
     * helper = FactoryUtil.createTableFactoryHelper(this, context);
     *
     * keyFormat = helper.discoverDecodingFormat(DeserializationFormatFactory.class, KEY_FORMAT);
     * valueFormat = helper.discoverDecodingFormat(DeserializationFormatFactory.class, VALUE_FORMAT);
     *
     * helper.validate();
     *
     * ... // construct connector with discovered formats
     * }</pre>
     *
     * <p>Note: The format option parameter of {@link
     * TableFactoryHelper#discoverEncodingFormat(Class, ConfigOption)} and {@link
     * TableFactoryHelper#discoverDecodingFormat(Class, ConfigOption)} must be {@link #FORMAT} or
     * end with {@link #FORMAT_SUFFIX}. The discovery logic will replace 'format' with the factory
     * identifier value as the format prefix. For example, assuming the identifier is 'json', if the
     * format option key is 'format', then the format prefix is 'json.'. If the format option key is
     * 'value.format', then the format prefix is 'value.json'. The format prefix is used to project
     * the options for the format factory.
     *
     * <p>Note: This utility checks for left-over options in the final step.
     */
    public static TableFactoryHelper createTableFactoryHelper(
            DynamicTableFactory factory, DynamicTableFactory.Context context) {
        return new TableFactoryHelper(factory, context);
    }

    /**
     * Discovers a factory using the given factory base class and identifier.
     *
     * <p>This method is meant for cases where {@link #createTableFactoryHelper(DynamicTableFactory,
     * DynamicTableFactory.Context)} {@link #createTableSource(Catalog, ObjectIdentifier,
     * CatalogTable, ReadableConfig, ClassLoader, boolean)}, and {@link #createTableSink(Catalog,
     * ObjectIdentifier, CatalogTable, ReadableConfig, ClassLoader, boolean)} are not applicable.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Factory> T discoverFactory(
            ClassLoader classLoader, Class<T> factoryClass, String factoryIdentifier) {
        final List<Factory> factories = discoverFactories(classLoader);

        final List<Factory> foundFactories =
                factories.stream()
                        .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                        .collect(Collectors.toList());

        if (foundFactories.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Could not find any factories that implement '%s' in the classpath.",
                            factoryClass.getName()));
        }

        final List<Factory> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.factoryIdentifier().equals(factoryIdentifier))
                        .collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Could not find any factory for identifier '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available factory identifiers are:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            foundFactories.stream()
                                    .map(Factory::factoryIdentifier)
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
        if (matchingFactories.size() > 1) {
            throw new ValidationException(
                    String.format(
                            "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
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
        validateFactoryOptions(factory.requiredOptions(), factory.optionalOptions(), options);
    }

    /**
     * Validates the required options and optional options.
     *
     * <p>Note: It does not check for left-over options.
     */
    public static void validateFactoryOptions(
            Set<ConfigOption<?>> requiredOptions,
            Set<ConfigOption<?>> optionalOptions,
            ReadableConfig options) {
        // currently Flink's options have no validation feature which is why we access them eagerly
        // to provoke a parsing error

        final List<String> missingRequiredOptions =
                requiredOptions.stream()
                        .filter(option -> readOption(options, option) == null)
                        .map(ConfigOption::key)
                        .sorted()
                        .collect(Collectors.toList());

        if (!missingRequiredOptions.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "One or more required options are missing.\n\n"
                                    + "Missing required options are:\n\n"
                                    + "%s",
                            String.join("\n", missingRequiredOptions)));
        }

        optionalOptions.forEach(option -> readOption(options, option));
    }

    /** Validates unconsumed option keys. */
    public static void validateUnconsumedKeys(
            String factoryIdentifier, Set<String> allOptionKeys, Set<String> consumedOptionKeys) {
        final Set<String> remainingOptionKeys = new HashSet<>(allOptionKeys);
        remainingOptionKeys.removeAll(consumedOptionKeys);
        if (remainingOptionKeys.size() > 0) {
            throw new ValidationException(
                    String.format(
                            "Unsupported options found for connector '%s'.\n\n"
                                    + "Unsupported options:\n\n"
                                    + "%s\n\n"
                                    + "Supported options:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            remainingOptionKeys.stream().sorted().collect(Collectors.joining("\n")),
                            consumedOptionKeys.stream()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static <T extends DynamicTableFactory> T getDynamicTableFactory(
            Class<T> factoryClass, @Nullable Catalog catalog, DefaultDynamicTableContext context) {
        // catalog factory has highest precedence
        if (catalog != null) {
            final Factory factory =
                    catalog.getFactory()
                            .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                            .orElse(null);
            if (factory != null) {
                return (T) factory;
            }
        }

        // fallback to factory discovery
        final String connectorOption = context.getCatalogTable().getOptions().get(CONNECTOR.key());
        if (connectorOption == null) {
            throw new ValidationException(
                    String.format(
                            "Table options do not contain an option key '%s' for discovering a connector.",
                            CONNECTOR.key()));
        }
        try {
            return discoverFactory(context.getClassLoader(), factoryClass, connectorOption);
        } catch (ValidationException e) {
            throw enrichNoMatchingConnectorError(factoryClass, context, connectorOption);
        }
    }

    private static ValidationException enrichNoMatchingConnectorError(
            Class<?> factoryClass, DefaultDynamicTableContext context, String connectorOption) {
        final DynamicTableFactory factory;
        try {
            factory =
                    discoverFactory(
                            context.getClassLoader(), DynamicTableFactory.class, connectorOption);
        } catch (ValidationException e) {
            return new ValidationException(
                    String.format(
                            "Cannot discover a connector using option: %s",
                            stringifyOption(CONNECTOR.key(), connectorOption)),
                    e);
        }

        final Class<?> sourceFactoryClass = DynamicTableSourceFactory.class;
        final Class<?> sinkFactoryClass = DynamicTableSinkFactory.class;
        // for a better exception message
        if (sourceFactoryClass.equals(factoryClass)
                && sinkFactoryClass.isAssignableFrom(factory.getClass())) {
            // discovering source, but not found, and this is a sink connector.
            return new ValidationException(
                    String.format(
                            "Connector '%s' can only be used as a sink. It cannot be used as a source.",
                            connectorOption));
        } else if (sinkFactoryClass.equals(factoryClass)
                && sourceFactoryClass.isAssignableFrom(factory.getClass())) {
            // discovering sink, but not found, and this is a source connector.
            return new ValidationException(
                    String.format(
                            "Connector '%s' can only be used as a source. It cannot be used as a sink.",
                            connectorOption));
        } else {
            return new ValidationException(
                    String.format(
                            "Connector '%s' does neither implement the '%s' nor the '%s' interface.",
                            connectorOption,
                            sourceFactoryClass.getName(),
                            sinkFactoryClass.getName()));
        }
    }

    private static List<Factory> discoverFactories(ClassLoader classLoader) {
        try {
            final List<Factory> result = new LinkedList<>();
            ServiceLoader.load(Factory.class, classLoader).iterator().forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for factories.", e);
            throw new TableException("Could not load service provider for factories.", e);
        }
    }

    private static String stringifyOption(String key, String value) {
        return String.format(
                "'%s'='%s'",
                EncodingUtils.escapeSingleQuotes(key), EncodingUtils.escapeSingleQuotes(value));
    }

    private static <T> T readOption(ReadableConfig options, ConfigOption<T> option) {
        try {
            return options.get(option);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format("Invalid value for option '%s'.", option.key()), t);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper classes
    // --------------------------------------------------------------------------------------------

    /**
     * Helper utility for discovering formats and validating all options for a {@link
     * DynamicTableFactory}.
     *
     * @see #createTableFactoryHelper(DynamicTableFactory, DynamicTableFactory.Context)
     */
    public static class TableFactoryHelper {

        private final DynamicTableFactory tableFactory;

        private final DynamicTableFactory.Context context;

        private final Configuration allOptions;

        private final Set<String> consumedOptionKeys;

        private TableFactoryHelper(
                DynamicTableFactory tableFactory, DynamicTableFactory.Context context) {
            this.tableFactory = tableFactory;
            this.context = context;
            this.allOptions = Configuration.fromMap(context.getCatalogTable().getOptions());
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
         * Discovers a {@link DecodingFormat} of the given type using the given option as factory
         * identifier.
         */
        public <I, F extends DecodingFormatFactory<I>> DecodingFormat<I> discoverDecodingFormat(
                Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
            return discoverOptionalDecodingFormat(formatFactoryClass, formatOption)
                    .orElseThrow(
                            () ->
                                    new ValidationException(
                                            String.format(
                                                    "Could not find required scan format '%s'.",
                                                    formatOption.key())));
        }

        /**
         * Discovers a {@link DecodingFormat} of the given type using the given option (if present)
         * as factory identifier.
         */
        public <I, F extends DecodingFormatFactory<I>>
                Optional<DecodingFormat<I>> discoverOptionalDecodingFormat(
                        Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
            return discoverOptionalFormatFactory(formatFactoryClass, formatOption)
                    .map(
                            formatFactory -> {
                                String formatPrefix = formatPrefix(formatFactory, formatOption);
                                try {
                                    return formatFactory.createDecodingFormat(
                                            context, projectOptions(formatPrefix));
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
         * Discovers a {@link EncodingFormat} of the given type using the given option as factory
         * identifier.
         */
        public <I, F extends EncodingFormatFactory<I>> EncodingFormat<I> discoverEncodingFormat(
                Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
            return discoverOptionalEncodingFormat(formatFactoryClass, formatOption)
                    .orElseThrow(
                            () ->
                                    new ValidationException(
                                            String.format(
                                                    "Could not find required sink format '%s'.",
                                                    formatOption.key())));
        }

        /**
         * Discovers a {@link EncodingFormat} of the given type using the given option (if present)
         * as factory identifier.
         */
        public <I, F extends EncodingFormatFactory<I>>
                Optional<EncodingFormat<I>> discoverOptionalEncodingFormat(
                        Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
            return discoverOptionalFormatFactory(formatFactoryClass, formatOption)
                    .map(
                            formatFactory -> {
                                String formatPrefix = formatPrefix(formatFactory, formatOption);
                                try {
                                    return formatFactory.createEncodingFormat(
                                            context, projectOptions(formatPrefix));
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
            validateUnconsumedKeys(
                    tableFactory.factoryIdentifier(), allOptions.keySet(), consumedOptionKeys);
        }

        /**
         * Validates the options of the {@link DynamicTableFactory}. It checks for unconsumed option
         * keys while ignoring the options with given prefixes.
         *
         * <p>The option keys that have given prefix {@code prefixToSkip} would just be skipped for
         * validation.
         *
         * @param prefixesToSkip Set of option key prefixes to skip validation
         */
        public void validateExcept(String... prefixesToSkip) {
            Preconditions.checkArgument(
                    prefixesToSkip.length > 0, "Prefixes to skip can not be empty.");
            final List<String> prefixesList = Arrays.asList(prefixesToSkip);
            consumedOptionKeys.addAll(
                    allOptions.keySet().stream()
                            .filter(key -> prefixesList.stream().anyMatch(key::startsWith))
                            .collect(Collectors.toSet()));
            validate();
        }

        /** Returns all options of the table. */
        public ReadableConfig getOptions() {
            return allOptions;
        }

        // ----------------------------------------------------------------------------------------

        private <F extends Factory> Optional<F> discoverOptionalFormatFactory(
                Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
            final String identifier = allOptions.get(formatOption);
            if (identifier == null) {
                return Optional.empty();
            }
            final F factory =
                    discoverFactory(context.getClassLoader(), formatFactoryClass, identifier);
            String formatPrefix = formatPrefix(factory, formatOption);
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

        private String formatPrefix(Factory formatFactory, ConfigOption<String> formatOption) {
            String identifier = formatFactory.factoryIdentifier();
            if (formatOption.key().equals(FORMAT.key())) {
                return identifier + ".";
            } else if (formatOption.key().endsWith(FORMAT_SUFFIX)) {
                // extract the key prefix, e.g. extract 'key' from 'key.format'
                String keyPrefix =
                        formatOption
                                .key()
                                .substring(0, formatOption.key().length() - FORMAT_SUFFIX.length());
                return keyPrefix + "." + identifier + ".";
            } else {
                throw new ValidationException(
                        "Format identifier key should be 'format' or suffix with '.format', "
                                + "don't support format identifier key '"
                                + formatOption.key()
                                + "'.");
            }
        }

        private ReadableConfig projectOptions(String formatPrefix) {
            return new DelegatingConfiguration(allOptions, formatPrefix);
        }
    }

    private static class DefaultDynamicTableContext implements DynamicTableFactory.Context {

        private final ObjectIdentifier objectIdentifier;
        private final CatalogTable catalogTable;
        private final ReadableConfig configuration;
        private final ClassLoader classLoader;
        private final boolean isTemporary;

        DefaultDynamicTableContext(
                ObjectIdentifier objectIdentifier,
                CatalogTable catalogTable,
                ReadableConfig configuration,
                ClassLoader classLoader,
                boolean isTemporary) {
            this.objectIdentifier = objectIdentifier;
            this.catalogTable = catalogTable;
            this.configuration = configuration;
            this.classLoader = classLoader;
            this.isTemporary = isTemporary;
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

        @Override
        public boolean isTemporary() {
            return isTemporary;
        }
    }

    // --------------------------------------------------------------------------------------------

    private FactoryUtil() {
        // no instantiation
    }
}
