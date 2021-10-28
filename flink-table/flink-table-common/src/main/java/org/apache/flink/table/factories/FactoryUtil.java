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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.FallbackKey;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.NoMatchingTableFactoryException;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.configuration.ConfigurationUtils.canBePrefixMap;
import static org.apache.flink.configuration.ConfigurationUtils.filterPrefixMapKey;
import static org.apache.flink.configuration.GlobalConfiguration.HIDDEN_CONTENT;
import static org.apache.flink.table.module.CommonModuleOptions.MODULE_TYPE;

/** Utility for working with {@link Factory}s. */
@PublicEvolving
public final class FactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FactoryUtil.class);

    /**
     * Describes the property version. This can be used for backwards compatibility in case the
     * property format changes.
     */
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
     * The placeholder symbol to be used for keys of options which can be templated. See {@link
     * Factory} for details.
     */
    public static final String PLACEHOLDER_SYMBOL = "#";

    /**
     * Creates a {@link DynamicTableSource} from a {@link CatalogTable}.
     *
     * <p>It considers {@link Catalog#getFactory()} if provided.
     */
    public static DynamicTableSource createTableSource(
            @Nullable Catalog catalog,
            ObjectIdentifier objectIdentifier,
            ResolvedCatalogTable catalogTable,
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
            ResolvedCatalogTable catalogTable,
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
     * Creates a utility that helps validating options for a {@link CatalogFactory}.
     *
     * <p>Note: This utility checks for left-over options in the final step.
     */
    public static CatalogFactoryHelper createCatalogFactoryHelper(
            CatalogFactory factory, CatalogFactory.Context context) {
        return new CatalogFactoryHelper(factory, context);
    }

    /**
     * Creates a utility that helps validating options for a {@link ModuleFactory}.
     *
     * <p>Note: This utility checks for left-over options in the final step.
     */
    public static ModuleFactoryHelper createModuleFactoryHelper(
            ModuleFactory factory, ModuleFactory.Context context) {
        return new ModuleFactoryHelper(factory, context);
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
     * Attempts to discover an appropriate catalog factory and creates an instance of the catalog.
     *
     * <p>This first uses the legacy {@link TableFactory} stack to discover a matching {@link
     * CatalogFactory}. If none is found, it falls back to the new stack using {@link Factory}
     * instead.
     */
    public static Catalog createCatalog(
            String catalogName,
            Map<String, String> options,
            ReadableConfig configuration,
            ClassLoader classLoader) {
        // Use the legacy mechanism first for compatibility
        try {
            final CatalogFactory legacyFactory =
                    TableFactoryService.find(CatalogFactory.class, options, classLoader);
            return legacyFactory.createCatalog(catalogName, options);
        } catch (NoMatchingTableFactoryException e) {
            // No matching legacy factory found, try using the new stack

            final DefaultCatalogContext discoveryContext =
                    new DefaultCatalogContext(catalogName, options, configuration, classLoader);
            try {
                final CatalogFactory factory = getCatalogFactory(discoveryContext);

                // The type option is only used for discovery, we don't actually want to forward it
                // to the catalog factory itself.
                final Map<String, String> factoryOptions =
                        options.entrySet().stream()
                                .filter(
                                        entry ->
                                                !CommonCatalogOptions.CATALOG_TYPE
                                                        .key()
                                                        .equals(entry.getKey()))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                final DefaultCatalogContext context =
                        new DefaultCatalogContext(
                                catalogName, factoryOptions, configuration, classLoader);
                return factory.createCatalog(context);
            } catch (Throwable t) {
                throw new ValidationException(
                        String.format(
                                "Unable to create catalog '%s'.%n%nCatalog options are:%n%s",
                                catalogName,
                                options.entrySet().stream()
                                        .map(
                                                optionEntry ->
                                                        stringifyOption(
                                                                optionEntry.getKey(),
                                                                optionEntry.getValue()))
                                        .sorted()
                                        .collect(Collectors.joining("\n"))),
                        t);
            }
        }
    }

    /**
     * Discovers a matching module factory and creates an instance of it.
     *
     * <p>This first uses the legacy {@link TableFactory} stack to discover a matching {@link
     * ModuleFactory}. If none is found, it falls back to the new stack using {@link Factory}
     * instead.
     */
    public static Module createModule(
            String moduleName,
            Map<String, String> options,
            ReadableConfig configuration,
            ClassLoader classLoader) {
        if (options.containsKey(MODULE_TYPE.key())) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' = '%s' is not supported since module name "
                                    + "is used to find module",
                            MODULE_TYPE.key(), options.get(MODULE_TYPE.key())));
        }

        try {
            final Map<String, String> optionsWithType = new HashMap<>(options);
            optionsWithType.put(MODULE_TYPE.key(), moduleName);

            final ModuleFactory legacyFactory =
                    TableFactoryService.find(ModuleFactory.class, optionsWithType, classLoader);
            return legacyFactory.createModule(optionsWithType);
        } catch (NoMatchingTableFactoryException e) {
            final DefaultModuleContext discoveryContext =
                    new DefaultModuleContext(options, configuration, classLoader);
            try {
                final ModuleFactory factory =
                        discoverFactory(
                                ((ModuleFactory.Context) discoveryContext).getClassLoader(),
                                ModuleFactory.class,
                                moduleName);

                final DefaultModuleContext context =
                        new DefaultModuleContext(options, configuration, classLoader);
                return factory.createModule(context);
            } catch (Throwable t) {
                throw new ValidationException(
                        String.format(
                                "Unable to create module '%s'.%n%nModule options are:%n%s",
                                moduleName,
                                options.entrySet().stream()
                                        .map(
                                                optionEntry ->
                                                        stringifyOption(
                                                                optionEntry.getKey(),
                                                                optionEntry.getValue()))
                                        .sorted()
                                        .collect(Collectors.joining("\n"))),
                        t);
            }
        }
    }

    /**
     * Discovers a factory using the given factory base class and identifier.
     *
     * <p>This method is meant for cases where {@link #createTableFactoryHelper(DynamicTableFactory,
     * DynamicTableFactory.Context)} {@link #createTableSource(Catalog, ObjectIdentifier,
     * ResolvedCatalogTable, ReadableConfig, ClassLoader, boolean)}, and {@link
     * #createTableSink(Catalog, ObjectIdentifier, ResolvedCatalogTable, ReadableConfig,
     * ClassLoader, boolean)} are not applicable.
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
                        // Templated options will never appear with their template key, so we need
                        // to ignore them as required properties here
                        .filter(
                                option ->
                                        allKeys(option)
                                                .noneMatch(k -> k.contains(PLACEHOLDER_SYMBOL)))
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
            String factoryIdentifier,
            Set<String> allOptionKeys,
            Set<String> consumedOptionKeys,
            Set<String> deprecatedOptionKeys) {
        final Set<String> remainingOptionKeys = new HashSet<>(allOptionKeys);
        remainingOptionKeys.removeAll(consumedOptionKeys);
        if (!remainingOptionKeys.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Unsupported options found for '%s'.\n\n"
                                    + "Unsupported options:\n\n"
                                    + "%s\n\n"
                                    + "Supported options:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            remainingOptionKeys.stream().sorted().collect(Collectors.joining("\n")),
                            consumedOptionKeys.stream()
                                    .map(
                                            k -> {
                                                if (deprecatedOptionKeys.contains(k)) {
                                                    return String.format("%s (deprecated)", k);
                                                }
                                                return k;
                                            })
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
    }

    /** Validates unconsumed option keys. */
    public static void validateUnconsumedKeys(
            String factoryIdentifier, Set<String> allOptionKeys, Set<String> consumedOptionKeys) {
        validateUnconsumedKeys(
                factoryIdentifier, allOptionKeys, consumedOptionKeys, Collections.emptySet());
    }

    /** Returns the required option prefix for options of the given format. */
    public static String getFormatPrefix(
            ConfigOption<String> formatOption, String formatIdentifier) {
        final String formatOptionKey = formatOption.key();
        if (formatOptionKey.equals(FORMAT.key())) {
            return formatIdentifier + ".";
        } else if (formatOptionKey.endsWith(FORMAT_SUFFIX)) {
            // extract the key prefix, e.g. extract 'key' from 'key.format'
            String keyPrefix =
                    formatOptionKey.substring(0, formatOptionKey.length() - FORMAT_SUFFIX.length());
            return keyPrefix + "." + formatIdentifier + ".";
        } else {
            throw new ValidationException(
                    "Format identifier key should be 'format' or suffix with '.format', "
                            + "don't support format identifier key '"
                            + formatOptionKey
                            + "'.");
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

    private static CatalogFactory getCatalogFactory(CatalogFactory.Context context) {
        final String catalogType =
                context.getOptions().get(CommonCatalogOptions.CATALOG_TYPE.key());
        if (catalogType == null) {
            throw new ValidationException(
                    String.format(
                            "Catalog options do not contain an option key '%s' for discovering a catalog.",
                            CommonCatalogOptions.CATALOG_TYPE.key()));
        }

        return discoverFactory(context.getClassLoader(), CatalogFactory.class, catalogType);
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
        if (GlobalConfiguration.isSensitive(key)) {
            value = HIDDEN_CONTENT;
        }
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

    private static Set<String> allKeysExpanded(ConfigOption<?> option, Set<String> actualKeys) {
        return allKeysExpanded("", option, actualKeys);
    }

    private static Set<String> allKeysExpanded(
            String prefix, ConfigOption<?> option, Set<String> actualKeys) {
        final Set<String> staticKeys =
                allKeys(option).map(k -> prefix + k).collect(Collectors.toSet());
        if (!canBePrefixMap(option)) {
            return staticKeys;
        }
        // include all prefix keys of a map option by considering the actually provided keys
        return Stream.concat(
                        staticKeys.stream(),
                        staticKeys.stream()
                                .flatMap(
                                        k ->
                                                actualKeys.stream()
                                                        .filter(c -> filterPrefixMapKey(k, c))))
                .collect(Collectors.toSet());
    }

    private static Stream<String> allKeys(ConfigOption<?> option) {
        return Stream.concat(Stream.of(option.key()), fallbackKeys(option));
    }

    private static Stream<String> fallbackKeys(ConfigOption<?> option) {
        return StreamSupport.stream(option.fallbackKeys().spliterator(), false)
                .map(FallbackKey::getKey);
    }

    private static Stream<String> deprecatedKeys(ConfigOption<?> option) {
        return StreamSupport.stream(option.fallbackKeys().spliterator(), false)
                .filter(FallbackKey::isDeprecated)
                .map(FallbackKey::getKey);
    }

    // --------------------------------------------------------------------------------------------
    // Helper classes
    // --------------------------------------------------------------------------------------------

    private static class FactoryHelper<F extends Factory> {

        protected final F factory;

        protected final Configuration allOptions;

        protected final Set<String> consumedOptionKeys;

        protected final Set<String> deprecatedOptionKeys;

        FactoryHelper(
                F factory, Map<String, String> configuration, ConfigOption<?>... implicitOptions) {
            this.factory = factory;
            this.allOptions = Configuration.fromMap(configuration);

            final List<ConfigOption<?>> consumedOptions = new ArrayList<>();
            consumedOptions.addAll(Arrays.asList(implicitOptions));
            consumedOptions.addAll(factory.requiredOptions());
            consumedOptions.addAll(factory.optionalOptions());

            consumedOptionKeys =
                    consumedOptions.stream()
                            .flatMap(
                                    option -> allKeysExpanded(option, allOptions.keySet()).stream())
                            .collect(Collectors.toSet());

            deprecatedOptionKeys =
                    consumedOptions.stream()
                            .flatMap(FactoryUtil::deprecatedKeys)
                            .collect(Collectors.toSet());
        }

        /** Validates the options of the factory. It checks for unconsumed option keys. */
        public void validate() {
            validateFactoryOptions(factory, allOptions);
            validateUnconsumedKeys(
                    factory.factoryIdentifier(),
                    allOptions.keySet(),
                    consumedOptionKeys,
                    deprecatedOptionKeys);
        }

        /**
         * Validates the options of the factory. It checks for unconsumed option keys while ignoring
         * the options with given prefixes.
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

        /** Returns all options currently being consumed by the factory. */
        public ReadableConfig getOptions() {
            return allOptions;
        }
    }

    /**
     * Helper utility for validating all options for a {@link CatalogFactory}.
     *
     * @see #createCatalogFactoryHelper(CatalogFactory, CatalogFactory.Context)
     */
    public static class CatalogFactoryHelper extends FactoryHelper<CatalogFactory> {

        public CatalogFactoryHelper(CatalogFactory catalogFactory, CatalogFactory.Context context) {
            super(catalogFactory, context.getOptions(), PROPERTY_VERSION);
        }
    }

    /**
     * Helper utility for validating all options for a {@link ModuleFactory}.
     *
     * @see #createModuleFactoryHelper(ModuleFactory, ModuleFactory.Context)
     */
    public static class ModuleFactoryHelper extends FactoryHelper<ModuleFactory> {
        public ModuleFactoryHelper(ModuleFactory moduleFactory, ModuleFactory.Context context) {
            super(moduleFactory, context.getOptions(), PROPERTY_VERSION);
        }
    }

    /**
     * Helper utility for discovering formats and validating all options for a {@link
     * DynamicTableFactory}.
     *
     * @see #createTableFactoryHelper(DynamicTableFactory, DynamicTableFactory.Context)
     */
    public static class TableFactoryHelper extends FactoryHelper<DynamicTableFactory> {

        private final DynamicTableFactory.Context context;

        private TableFactoryHelper(
                DynamicTableFactory tableFactory, DynamicTableFactory.Context context) {
            super(
                    tableFactory,
                    context.getCatalogTable().getOptions(),
                    PROPERTY_VERSION,
                    CONNECTOR);
            this.context = context;
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
            final List<ConfigOption<?>> consumedOptions = new ArrayList<>();
            consumedOptions.addAll(factory.requiredOptions());
            consumedOptions.addAll(factory.optionalOptions());

            consumedOptions.stream()
                    .flatMap(
                            option ->
                                    allKeysExpanded(formatPrefix, option, allOptions.keySet())
                                            .stream())
                    .forEach(consumedOptionKeys::add);

            consumedOptions.stream()
                    .flatMap(FactoryUtil::deprecatedKeys)
                    .map(k -> formatPrefix + k)
                    .forEach(deprecatedOptionKeys::add);

            return Optional.of(factory);
        }

        private String formatPrefix(Factory formatFactory, ConfigOption<String> formatOption) {
            String identifier = formatFactory.factoryIdentifier();
            return getFormatPrefix(formatOption, identifier);
        }

        private ReadableConfig projectOptions(String formatPrefix) {
            return new DelegatingConfiguration(allOptions, formatPrefix);
        }
    }

    /** Default implementation of {@link DynamicTableFactory.Context}. */
    @Internal
    public static class DefaultDynamicTableContext implements DynamicTableFactory.Context {

        private final ObjectIdentifier objectIdentifier;
        private final ResolvedCatalogTable catalogTable;
        private final ReadableConfig configuration;
        private final ClassLoader classLoader;
        private final boolean isTemporary;

        public DefaultDynamicTableContext(
                ObjectIdentifier objectIdentifier,
                ResolvedCatalogTable catalogTable,
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
        public ResolvedCatalogTable getCatalogTable() {
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

    /** Default implementation of {@link CatalogFactory.Context}. */
    @Internal
    public static class DefaultCatalogContext implements CatalogFactory.Context {
        private final String name;
        private final Map<String, String> options;
        private final ReadableConfig configuration;
        private final ClassLoader classLoader;

        public DefaultCatalogContext(
                String name,
                Map<String, String> options,
                ReadableConfig configuration,
                ClassLoader classLoader) {
            this.name = name;
            this.options = options;
            this.configuration = configuration;
            this.classLoader = classLoader;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Map<String, String> getOptions() {
            return options;
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

    /** Default implementation of {@link ModuleFactory.Context}. */
    @Internal
    public static class DefaultModuleContext implements ModuleFactory.Context {
        private final Map<String, String> options;
        private final ReadableConfig configuration;
        private final ClassLoader classLoader;

        public DefaultModuleContext(
                Map<String, String> options,
                ReadableConfig configuration,
                ClassLoader classLoader) {
            this.options = options;
            this.configuration = configuration;
            this.classLoader = classLoader;
        }

        @Override
        public Map<String, String> getOptions() {
            return options;
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
