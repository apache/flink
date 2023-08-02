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

package org.apache.flink.docs.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.util.function.BiConsumerWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Utility for discovering config options. */
public class ConfigurationOptionLocator {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationOptionLocator.class);

    private static final OptionsClassLocation[] LOCATIONS =
            new OptionsClassLocation[] {
                new OptionsClassLocation("flink-core", "org.apache.flink.configuration"),
                new OptionsClassLocation("flink-runtime", "org.apache.flink.runtime.shuffle"),
                new OptionsClassLocation("flink-runtime", "org.apache.flink.runtime.jobgraph"),
                new OptionsClassLocation(
                        "flink-runtime", "org.apache.flink.runtime.highavailability"),
                new OptionsClassLocation(
                        "flink-streaming-java", "org.apache.flink.streaming.api.environment"),
                new OptionsClassLocation("flink-yarn", "org.apache.flink.yarn.configuration"),
                new OptionsClassLocation(
                        "flink-metrics/flink-metrics-prometheus",
                        "org.apache.flink.metrics.prometheus"),
                new OptionsClassLocation(
                        "flink-metrics/flink-metrics-influxdb",
                        "org.apache.flink.metrics.influxdb"),
                new OptionsClassLocation(
                        "flink-state-backends/flink-statebackend-rocksdb",
                        "org.apache.flink.contrib.streaming.state"),
                new OptionsClassLocation(
                        "flink-table/flink-table-api-java", "org.apache.flink.table.api.config"),
                new OptionsClassLocation("flink-python", "org.apache.flink.python"),
                new OptionsClassLocation(
                        "flink-kubernetes", "org.apache.flink.kubernetes.configuration"),
                new OptionsClassLocation("flink-clients", "org.apache.flink.client.cli"),
                new OptionsClassLocation(
                        "flink-table/flink-sql-client", "org.apache.flink.table.client.config"),
                new OptionsClassLocation(
                        "flink-libraries/flink-cep", "org.apache.flink.cep.configuration"),
                new OptionsClassLocation(
                        "flink-dstl/flink-dstl-dfs", "org.apache.flink.changelog.fs"),
                new OptionsClassLocation(
                        "flink-table/flink-sql-gateway", "org.apache.flink.table.gateway.rest.util")
            };

    private static final Set<String> EXCLUSIONS =
            new HashSet<>(
                    Arrays.asList(
                            "org.apache.flink.configuration.ReadableConfig",
                            "org.apache.flink.configuration.WritableConfig",
                            "org.apache.flink.configuration.ConfigOptions",
                            "org.apache.flink.streaming.api.environment.CheckpointConfig",
                            "org.apache.flink.contrib.streaming.state.PredefinedOptions",
                            "org.apache.flink.python.PythonConfig",
                            "org.apache.flink.cep.configuration.SharedBufferCacheConfig"));

    private static final String DEFAULT_PATH_PREFIX = "src/main/java";

    private static final String CLASS_NAME_GROUP = "className";
    private static final String CLASS_PREFIX_GROUP = "classPrefix";
    private static final Pattern CLASS_NAME_PATTERN =
            Pattern.compile(
                    "(?<"
                            + CLASS_NAME_GROUP
                            + ">(?<"
                            + CLASS_PREFIX_GROUP
                            + ">[a-zA-Z]*)(?:Options|Config|Parameters))(?:\\.java)?");

    private final OptionsClassLocation[] locations;
    private final String pathPrefix;

    public ConfigurationOptionLocator() {
        this(LOCATIONS, DEFAULT_PATH_PREFIX);
    }

    public ConfigurationOptionLocator(OptionsClassLocation[] locations, String pathPrefix) {
        this.locations = locations;
        this.pathPrefix = pathPrefix;
    }

    public void discoverOptionsAndApply(
            Path rootDir,
            BiConsumerWithException<Class<?>, Collection<OptionWithMetaInfo>, ? extends Exception>
                    optionsConsumer)
            throws Exception {

        LOG.info(
                "Searching the following locations; configured via {}#LOCATIONS:{}",
                ConfigurationOptionLocator.class.getCanonicalName(),
                Arrays.stream(LOCATIONS)
                        .map(OptionsClassLocation::toString)
                        .collect(Collectors.joining("\n\t", "\n\t", "")));
        LOG.info(
                "Excluding the following classes; configured via {}#EXCLUSIONS:{}",
                ConfigurationOptionLocator.class.getCanonicalName(),
                EXCLUSIONS.stream().collect(Collectors.joining("\n\t", "\n\t", "")));

        for (OptionsClassLocation location : locations) {
            List<Class<?>> classes =
                    discoverOptionsAndApply(
                            rootDir, location.getModule(), location.getPackage(), pathPrefix);
            for (Class<?> clazz : classes) {
                List<OptionWithMetaInfo> optionWithMetaInfos = extractConfigOptions(clazz);
                optionsConsumer.accept(clazz, optionWithMetaInfos);
            }
        }
    }

    public static List<Class<?>> discoverOptionsAndApply(
            Path rootDir, String module, String packageName, String pathPrefix)
            throws IOException, ClassNotFoundException {

        Path configDir =
                rootDir.resolve(Paths.get(module, pathPrefix, packageName.replaceAll("\\.", "/")));

        List<Class<?>> optionClasses = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(configDir)) {
            for (Path entry : stream) {
                String fileName = entry.getFileName().toString();
                Matcher matcher = CLASS_NAME_PATTERN.matcher(fileName);
                if (matcher.matches()) {
                    final String className = packageName + '.' + matcher.group(CLASS_NAME_GROUP);

                    if (!EXCLUSIONS.contains(className)) {
                        Class<?> optionsClass = Class.forName(className);
                        if (shouldBeDocumented(optionsClass)) {
                            optionClasses.add(optionsClass);
                        }
                    }
                }
            }
        }
        return optionClasses;
    }

    private static boolean shouldBeDocumented(Class<?> clazz) {
        return clazz.getAnnotation(Deprecated.class) == null
                && clazz.getAnnotation(Documentation.ExcludeFromDocumentation.class) == null;
    }

    @VisibleForTesting
    public static List<OptionWithMetaInfo> extractConfigOptions(Class<?> clazz) {
        try {
            List<OptionWithMetaInfo> configOptions = new ArrayList<>(8);
            Field[] fields = clazz.getFields();
            for (Field field : fields) {
                if (isConfigOption(field)) {
                    configOptions.add(
                            new OptionWithMetaInfo((ConfigOption<?>) field.get(null), field));
                }
            }

            return configOptions;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to extract config options from class " + clazz + '.', e);
        }
    }

    private static boolean isConfigOption(Field field) {
        return field.getType().equals(ConfigOption.class);
    }
}
