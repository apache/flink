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

package org.apache.flink.docs.configuration;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.Formatter;
import org.apache.flink.configuration.description.HtmlFormatter;
import org.apache.flink.util.TimeUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.docs.util.Utils.escapeCharacters;

/** Class used for generating code based documentation of configuration parameters. */
public class ConfigOptionsDocGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigOptionsDocGenerator.class);

    static final OptionsClassLocation[] LOCATIONS =
            new OptionsClassLocation[] {
                new OptionsClassLocation("flink-core", "org.apache.flink.configuration"),
                new OptionsClassLocation("flink-runtime", "org.apache.flink.runtime.shuffle"),
                new OptionsClassLocation("flink-runtime", "org.apache.flink.runtime.jobgraph"),
                new OptionsClassLocation(
                        "flink-streaming-java", "org.apache.flink.streaming.api.environment"),
                new OptionsClassLocation("flink-yarn", "org.apache.flink.yarn.configuration"),
                new OptionsClassLocation("flink-mesos", "org.apache.flink.mesos.configuration"),
                new OptionsClassLocation(
                        "flink-mesos", "org.apache.flink.mesos.runtime.clusterframework"),
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
                new OptionsClassLocation("flink-clients", "org.apache.flink.client.cli")
            };

    static final Set<String> EXCLUSIONS =
            new HashSet<>(
                    Arrays.asList(
                            "org.apache.flink.configuration.ReadableConfig",
                            "org.apache.flink.configuration.WritableConfig",
                            "org.apache.flink.configuration.ConfigOptions",
                            "org.apache.flink.streaming.api.environment.CheckpointConfig",
                            "org.apache.flink.contrib.streaming.state.PredefinedOptions",
                            "org.apache.flink.python.PythonConfig"));

    static final String DEFAULT_PATH_PREFIX = "src/main/java";

    @VisibleForTesting static final String COMMON_SECTION_FILE_NAME = "common_section.html";

    private static final String CLASS_NAME_GROUP = "className";
    private static final String CLASS_PREFIX_GROUP = "classPrefix";
    private static final Pattern CLASS_NAME_PATTERN =
            Pattern.compile(
                    "(?<"
                            + CLASS_NAME_GROUP
                            + ">(?<"
                            + CLASS_PREFIX_GROUP
                            + ">[a-zA-Z]*)(?:Options|Config|Parameters))(?:\\.java)?");

    private static final Formatter formatter = new HtmlFormatter();
    /**
     * This method generates html tables from set of classes containing {@link ConfigOption
     * ConfigOptions}.
     *
     * <p>For each class 1 or more html tables will be generated and placed into a separate file,
     * depending on whether the class is annotated with {@link ConfigGroups}. The tables contain the
     * key, default value and description for every {@link ConfigOption}.
     *
     * <p>One additional table is generated containing all {@link ConfigOption ConfigOptions} that
     * are annotated with {@link Documentation.Section}.
     *
     * @param args [0] output directory for the generated files [1] project root directory
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String outputDirectory = args[0];
        String rootDir = args[1];

        LOG.info(
                "Searching the following locations; configured via {}#LOCATIONS:{}",
                ConfigOptionsDocGenerator.class.getCanonicalName(),
                Arrays.stream(LOCATIONS)
                        .map(OptionsClassLocation::toString)
                        .collect(Collectors.joining("\n\t", "\n\t", "")));
        LOG.info(
                "Excluding the following classes; configured via {}#EXCLUSIONS:{}",
                ConfigOptionsDocGenerator.class.getCanonicalName(),
                EXCLUSIONS.stream().collect(Collectors.joining("\n\t", "\n\t", "")));

        for (OptionsClassLocation location : LOCATIONS) {
            createTable(
                    rootDir,
                    location.getModule(),
                    location.getPackage(),
                    outputDirectory,
                    DEFAULT_PATH_PREFIX);
        }

        generateCommonSection(rootDir, outputDirectory, LOCATIONS, DEFAULT_PATH_PREFIX);
    }

    @VisibleForTesting
    static void generateCommonSection(
            String rootDir,
            String outputDirectory,
            OptionsClassLocation[] locations,
            String pathPrefix)
            throws IOException, ClassNotFoundException {
        List<OptionWithMetaInfo> allSectionOptions = new ArrayList<>(32);
        for (OptionsClassLocation location : locations) {
            allSectionOptions.addAll(
                    findSectionOptions(
                            rootDir, location.getModule(), location.getPackage(), pathPrefix));
        }

        Map<String, List<OptionWithMetaInfo>> optionsGroupedBySection =
                allSectionOptions.stream()
                        .flatMap(
                                option -> {
                                    final String[] sections =
                                            option.field
                                                    .getAnnotation(Documentation.Section.class)
                                                    .value();
                                    if (sections.length == 0) {
                                        throw new RuntimeException(
                                                String.format(
                                                        "Option %s is annotated with %s but the list of sections is empty.",
                                                        option.option.key(),
                                                        Documentation.Section.class
                                                                .getSimpleName()));
                                    }

                                    return Arrays.stream(sections)
                                            .map(section -> Tuple2.of(section, option));
                                })
                        .collect(
                                Collectors.groupingBy(
                                        option -> option.f0,
                                        Collectors.mapping(
                                                option -> option.f1, Collectors.toList())));

        optionsGroupedBySection.forEach(
                (section, options) -> {
                    options.sort(
                            (o1, o2) -> {
                                int position1 =
                                        o1.field
                                                .getAnnotation(Documentation.Section.class)
                                                .position();
                                int position2 =
                                        o2.field
                                                .getAnnotation(Documentation.Section.class)
                                                .position();
                                if (position1 == position2) {
                                    return o1.option.key().compareTo(o2.option.key());
                                } else {
                                    return Integer.compare(position1, position2);
                                }
                            });

                    String sectionHtmlTable = toHtmlTable(options);
                    try {
                        Files.write(
                                Paths.get(outputDirectory, getSectionFileName(section)),
                                sectionHtmlTable.getBytes(StandardCharsets.UTF_8));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @VisibleForTesting
    static String getSectionFileName(String section) {
        return section + "_section.html";
    }

    private static Collection<OptionWithMetaInfo> findSectionOptions(
            String rootDir, String module, String packageName, String pathPrefix)
            throws IOException, ClassNotFoundException {
        Collection<OptionWithMetaInfo> commonOptions = new ArrayList<>(32);
        processConfigOptions(
                rootDir,
                module,
                packageName,
                pathPrefix,
                optionsClass ->
                        extractConfigOptions(optionsClass).stream()
                                .filter(
                                        optionWithMetaInfo ->
                                                optionWithMetaInfo.field.getAnnotation(
                                                                Documentation.Section.class)
                                                        != null)
                                .forEachOrdered(commonOptions::add));
        return commonOptions;
    }

    private static void createTable(
            String rootDir,
            String module,
            String packageName,
            String outputDirectory,
            String pathPrefix)
            throws IOException, ClassNotFoundException {
        processConfigOptions(
                rootDir,
                module,
                packageName,
                pathPrefix,
                optionsClass -> {
                    List<Tuple2<ConfigGroup, String>> tables = generateTablesForClass(optionsClass);
                    for (Tuple2<ConfigGroup, String> group : tables) {
                        String name;
                        if (group.f0 == null) {
                            Matcher matcher =
                                    CLASS_NAME_PATTERN.matcher(optionsClass.getSimpleName());
                            if (!matcher.matches()) {
                                throw new RuntimeException(
                                        "Pattern did not match for "
                                                + optionsClass.getSimpleName()
                                                + '.');
                            }
                            name = matcher.group(CLASS_PREFIX_GROUP);
                        } else {
                            name = group.f0.name();
                        }

                        String outputFile = toSnakeCase(name) + "_configuration.html";
                        Files.write(
                                Paths.get(outputDirectory, outputFile),
                                group.f1.getBytes(StandardCharsets.UTF_8));
                    }
                });
    }

    @VisibleForTesting
    static String toSnakeCase(String name) {
        return name.replaceAll("(.)([A-Z][a-z])", "$1_$2").toLowerCase();
    }

    @VisibleForTesting
    static void processConfigOptions(
            String rootDir,
            String module,
            String packageName,
            String pathPrefix,
            ThrowingConsumer<Class<?>, IOException> classConsumer)
            throws IOException, ClassNotFoundException {
        Path configDir = Paths.get(rootDir, module, pathPrefix, packageName.replaceAll("\\.", "/"));

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(configDir)) {
            for (Path entry : stream) {
                String fileName = entry.getFileName().toString();
                Matcher matcher = CLASS_NAME_PATTERN.matcher(fileName);
                if (matcher.matches()) {
                    final String className = packageName + '.' + matcher.group(CLASS_NAME_GROUP);

                    if (!EXCLUSIONS.contains(className)) {
                        Class<?> optionsClass = Class.forName(className);
                        classConsumer.accept(optionsClass);
                    }
                }
            }
        }
    }

    @VisibleForTesting
    static List<Tuple2<ConfigGroup, String>> generateTablesForClass(Class<?> optionsClass) {
        ConfigGroups configGroups = optionsClass.getAnnotation(ConfigGroups.class);
        List<OptionWithMetaInfo> allOptions = extractConfigOptions(optionsClass);

        if (allOptions.isEmpty()) {
            return Collections.emptyList();
        }

        List<Tuple2<ConfigGroup, String>> tables;
        if (configGroups != null) {
            tables = new ArrayList<>(configGroups.groups().length + 1);
            Tree tree = new Tree(configGroups.groups(), allOptions);

            for (ConfigGroup group : configGroups.groups()) {
                List<OptionWithMetaInfo> configOptions = tree.findConfigOptions(group);
                if (!configOptions.isEmpty()) {
                    sortOptions(configOptions);
                    tables.add(Tuple2.of(group, toHtmlTable(configOptions)));
                }
            }
            List<OptionWithMetaInfo> configOptions = tree.getDefaultOptions();
            if (!configOptions.isEmpty()) {
                sortOptions(configOptions);
                tables.add(Tuple2.of(null, toHtmlTable(configOptions)));
            }
        } else {
            sortOptions(allOptions);
            tables = Collections.singletonList(Tuple2.of(null, toHtmlTable(allOptions)));
        }
        return tables;
    }

    @VisibleForTesting
    static List<OptionWithMetaInfo> extractConfigOptions(Class<?> clazz) {
        try {
            List<OptionWithMetaInfo> configOptions = new ArrayList<>(8);
            Field[] fields = clazz.getFields();
            for (Field field : fields) {
                if (isConfigOption(field) && shouldBeDocumented(field)) {
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

    private static boolean shouldBeDocumented(Field field) {
        return field.getAnnotation(Deprecated.class) == null
                && field.getAnnotation(Documentation.ExcludeFromDocumentation.class) == null;
    }

    /**
     * Transforms this configuration group into HTML formatted table. Options are sorted
     * alphabetically by key.
     *
     * @param options list of options to include in this group
     * @return string containing HTML formatted table
     */
    private static String toHtmlTable(final List<OptionWithMetaInfo> options) {
        StringBuilder htmlTable = new StringBuilder();
        htmlTable.append("<table class=\"configuration table table-bordered\">\n");
        htmlTable.append("    <thead>\n");
        htmlTable.append("        <tr>\n");
        htmlTable.append("            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n");
        htmlTable.append("            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n");
        htmlTable.append("            <th class=\"text-left\" style=\"width: 10%\">Type</th>\n");
        htmlTable.append(
                "            <th class=\"text-left\" style=\"width: 55%\">Description</th>\n");
        htmlTable.append("        </tr>\n");
        htmlTable.append("    </thead>\n");
        htmlTable.append("    <tbody>\n");

        for (OptionWithMetaInfo option : options) {
            htmlTable.append(toHtmlString(option));
        }

        htmlTable.append("    </tbody>\n");
        htmlTable.append("</table>\n");

        return htmlTable.toString();
    }

    /**
     * Transforms option to table row.
     *
     * @param optionWithMetaInfo option to transform
     * @return row with the option description
     */
    private static String toHtmlString(final OptionWithMetaInfo optionWithMetaInfo) {
        ConfigOption<?> option = optionWithMetaInfo.option;
        String defaultValue = stringifyDefault(optionWithMetaInfo);
        String type = typeToHtml(optionWithMetaInfo);
        Documentation.TableOption tableOption =
                optionWithMetaInfo.field.getAnnotation(Documentation.TableOption.class);
        StringBuilder execModeStringBuilder = new StringBuilder();
        if (tableOption != null) {
            Documentation.ExecMode execMode = tableOption.execMode();
            if (Documentation.ExecMode.BATCH_STREAMING.equals(execMode)) {
                execModeStringBuilder
                        .append("<br> <span class=\"label label-primary\">")
                        .append(Documentation.ExecMode.BATCH.toString())
                        .append("</span> <span class=\"label label-primary\">")
                        .append(Documentation.ExecMode.STREAMING.toString())
                        .append("</span>");
            } else {
                execModeStringBuilder
                        .append("<br> <span class=\"label label-primary\">")
                        .append(execMode.toString())
                        .append("</span>");
            }
        }

        return ""
                + "        <tr>\n"
                + "            <td><h5>"
                + escapeCharacters(option.key())
                + "</h5>"
                + execModeStringBuilder.toString()
                + "</td>\n"
                + "            <td style=\"word-wrap: break-word;\">"
                + escapeCharacters(addWordBreakOpportunities(defaultValue))
                + "</td>\n"
                + "            <td>"
                + type
                + "</td>\n"
                + "            <td>"
                + formatter.format(option.description())
                + "</td>\n"
                + "        </tr>\n";
    }

    private static Class<?> getClazz(ConfigOption<?> option) {
        try {
            Method getClazzMethod = ConfigOption.class.getDeclaredMethod("getClazz");
            getClazzMethod.setAccessible(true);
            Class clazz = (Class) getClazzMethod.invoke(option);
            getClazzMethod.setAccessible(false);
            return clazz;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isList(ConfigOption<?> option) {
        try {
            Method getClazzMethod = ConfigOption.class.getDeclaredMethod("isList");
            getClazzMethod.setAccessible(true);
            boolean isList = (boolean) getClazzMethod.invoke(option);
            getClazzMethod.setAccessible(false);
            return isList;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static String typeToHtml(OptionWithMetaInfo optionWithMetaInfo) {
        ConfigOption<?> option = optionWithMetaInfo.option;
        Class<?> clazz = getClazz(option);
        boolean isList = isList(option);

        if (clazz.isEnum()) {
            return enumTypeToHtml(clazz, isList);
        }

        return atomicTypeToHtml(clazz, isList);
    }

    private static String atomicTypeToHtml(Class<?> clazz, boolean isList) {
        String typeName = clazz.getSimpleName();

        final String type;
        if (isList) {
            type = String.format("List<%s>", typeName);
        } else {
            type = typeName;
        }

        return escapeCharacters(type);
    }

    private static String enumTypeToHtml(Class<?> enumClazz, boolean isList) {
        final String type;
        if (isList) {
            type = "List<Enum>";
        } else {
            type = "Enum";
        }

        return String.format(
                "<p>%s</p>Possible values: %s",
                escapeCharacters(type),
                escapeCharacters(Arrays.toString(enumClazz.getEnumConstants())));
    }

    @VisibleForTesting
    static String stringifyDefault(OptionWithMetaInfo optionWithMetaInfo) {
        ConfigOption<?> option = optionWithMetaInfo.option;
        Documentation.OverrideDefault overrideDocumentedDefault =
                optionWithMetaInfo.field.getAnnotation(Documentation.OverrideDefault.class);
        if (overrideDocumentedDefault != null) {
            return overrideDocumentedDefault.value();
        } else {
            Object value = option.defaultValue();
            return stringifyObject(value);
        }
    }

    @SuppressWarnings("unchecked")
    private static String stringifyObject(Object value) {
        if (value instanceof String) {
            if (((String) value).isEmpty()) {
                return "(none)";
            }
            return "\"" + value + "\"";
        } else if (value instanceof Duration) {
            return TimeUtils.formatWithHighestUnit((Duration) value);
        } else if (value instanceof List) {
            return ((List<Object>) value)
                    .stream()
                            .map(ConfigOptionsDocGenerator::stringifyObject)
                            .collect(Collectors.joining(";"));
        } else if (value instanceof Map) {
            return ((Map<String, String>) value)
                    .entrySet().stream()
                            .map(e -> String.format("%s:%s", e.getKey(), e.getValue()))
                            .collect(Collectors.joining(","));
        }
        return value == null ? "(none)" : value.toString();
    }

    private static String addWordBreakOpportunities(String value) {
        return value
                // allow breaking of semicolon separated lists
                .replace(";", ";<wbr>");
    }

    private static void sortOptions(List<OptionWithMetaInfo> configOptions) {
        configOptions.sort(Comparator.comparing(option -> option.option.key()));
    }

    /**
     * Data structure used to assign {@link ConfigOption ConfigOptions} to the {@link ConfigGroup}
     * with the longest matching prefix.
     */
    private static class Tree {
        private final Node root = new Node();

        Tree(ConfigGroup[] groups, Collection<OptionWithMetaInfo> options) {
            // generate a tree based on all key prefixes
            for (ConfigGroup group : groups) {
                String[] keyComponents = group.keyPrefix().split("\\.");
                Node currentNode = root;
                for (String keyComponent : keyComponents) {
                    currentNode = currentNode.addChild(keyComponent);
                }
                currentNode.markAsGroupRoot();
            }

            // assign options to their corresponding group, i.e. the last group root node
            // encountered when traversing
            // the tree based on the option key
            for (OptionWithMetaInfo option : options) {
                findGroupRoot(option.option.key()).assignOption(option);
            }
        }

        List<OptionWithMetaInfo> findConfigOptions(ConfigGroup configGroup) {
            Node groupRoot = findGroupRoot(configGroup.keyPrefix());
            return groupRoot.getConfigOptions();
        }

        List<OptionWithMetaInfo> getDefaultOptions() {
            return root.getConfigOptions();
        }

        private Node findGroupRoot(String key) {
            String[] keyComponents = key.split("\\.");
            Node lastRootNode = root;
            Node currentNode = root;
            for (String keyComponent : keyComponents) {
                final Node childNode = currentNode.getChild(keyComponent);
                if (childNode == null) {
                    break;
                } else {
                    currentNode = childNode;
                    if (currentNode.isGroupRoot()) {
                        lastRootNode = currentNode;
                    }
                }
            }
            return lastRootNode;
        }

        private static class Node {
            private final List<OptionWithMetaInfo> configOptions = new ArrayList<>(8);
            private final Map<String, Node> children = new HashMap<>(8);
            private boolean isGroupRoot = false;

            private Node addChild(String keyComponent) {
                Node child = children.get(keyComponent);
                if (child == null) {
                    child = new Node();
                    children.put(keyComponent, child);
                }
                return child;
            }

            private Node getChild(String keyComponent) {
                return children.get(keyComponent);
            }

            private void assignOption(OptionWithMetaInfo option) {
                configOptions.add(option);
            }

            private boolean isGroupRoot() {
                return isGroupRoot;
            }

            private void markAsGroupRoot() {
                this.isGroupRoot = true;
            }

            private List<OptionWithMetaInfo> getConfigOptions() {
                return configOptions;
            }
        }
    }

    static class OptionWithMetaInfo {
        final ConfigOption<?> option;
        final Field field;

        public OptionWithMetaInfo(ConfigOption<?> option, Field field) {
            this.option = option;
            this.field = field;
        }
    }

    private ConfigOptionsDocGenerator() {}
}
