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
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class used for generating code based documentation of configuration parameters.
 */
public class ConfigOptionsDocGenerator {

	static final OptionsClassLocation[] LOCATIONS = new OptionsClassLocation[]{
		new OptionsClassLocation("flink-core", "org.apache.flink.configuration"),
		new OptionsClassLocation("flink-runtime", "org.apache.flink.runtime.io.network.netty"),
		new OptionsClassLocation("flink-yarn", "org.apache.flink.yarn.configuration"),
		new OptionsClassLocation("flink-mesos", "org.apache.flink.mesos.configuration"),
		new OptionsClassLocation("flink-mesos", "org.apache.flink.mesos.runtime.clusterframework"),
	};

	private static final String CLASS_NAME_GROUP = "className";
	private static final String CLASS_PREFIX_GROUP = "classPrefix";
	private static final Pattern CLASS_NAME_PATTERN = Pattern.compile("(?<" + CLASS_NAME_GROUP + ">(?<" + CLASS_PREFIX_GROUP + ">[a-zA-Z]*)(?:Options|Config|Parameters))(?:\\.java)?");

	/**
	 * Placeholder that is used to prevent certain sections from being escaped. We don't need a sophisticated value
	 * but only something that won't show up in config options.
	 */
	private static final String TEMPORARY_PLACEHOLDER = "superRandomTemporaryPlaceholder";

	/**
	 * This method generates html tables from set of classes containing {@link ConfigOption ConfigOptions}.
	 *
	 * <p>For each class 1 or more html tables will be generated and placed into a separate file, depending on whether
	 * the class is annotated with {@link ConfigGroups}. The tables contain the key, default value and description for
	 * every {@link ConfigOption}.
	 *
	 * @param args
	 *  [0] output directory for the generated files
	 *  [1] project root directory
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		String outputDirectory = args[0];
		String rootDir = args[1];

		for (OptionsClassLocation location : LOCATIONS) {
			createTable(rootDir, location.getModule(), location.getPackage(), outputDirectory);
		}
	}

	private static void createTable(String rootDir, String module, String packageName, String outputDirectory) throws IOException, ClassNotFoundException {
		processConfigOptions(rootDir, module, packageName, optionsClass -> {
			List<Tuple2<ConfigGroup, String>> tables = generateTablesForClass(optionsClass);
			for (Tuple2<ConfigGroup, String> group : tables) {
				String name;
				if (group.f0 == null) {
					Matcher matcher = CLASS_NAME_PATTERN.matcher(optionsClass.getSimpleName());
					if (!matcher.matches()) {
						throw new RuntimeException("Pattern did not match for " + optionsClass.getSimpleName() + '.');
					}
					name = matcher.group(CLASS_PREFIX_GROUP).replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();
				} else {
					name = group.f0.name().replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();
				}

				String outputFile = name + "_configuration.html";
				Files.write(Paths.get(outputDirectory, outputFile), group.f1.getBytes(StandardCharsets.UTF_8));
			}
		});
	}

	static void processConfigOptions(String rootDir, String module, String packageName, ThrowingConsumer<Class<?>, IOException> classConsumer) throws IOException, ClassNotFoundException {
		Path configDir = Paths.get(rootDir, module, "src/main/java", packageName.replaceAll("\\.", "/"));

		try (DirectoryStream<Path> stream = Files.newDirectoryStream(configDir)) {
			for (Path entry : stream) {
				String fileName = entry.getFileName().toString();
				Matcher matcher = CLASS_NAME_PATTERN.matcher(fileName);
				if (!fileName.equals("ConfigOptions.java") && matcher.matches()) {
					Class<?> optionsClass = Class.forName(packageName + '.' + matcher.group(CLASS_NAME_GROUP));
					classConsumer.accept(optionsClass);
				}
			}
		}
	}

	@VisibleForTesting
	static List<Tuple2<ConfigGroup, String>> generateTablesForClass(Class<?> optionsClass) {
		ConfigGroups configGroups = optionsClass.getAnnotation(ConfigGroups.class);
		List<OptionWithMetaInfo> allOptions = extractConfigOptions(optionsClass);

		List<Tuple2<ConfigGroup, String>> tables;
		if (configGroups != null) {
			tables = new ArrayList<>(configGroups.groups().length + 1);
			Tree tree = new Tree(configGroups.groups(), allOptions);

			for (ConfigGroup group : configGroups.groups()) {
				List<OptionWithMetaInfo> configOptions = tree.findConfigOptions(group);
				sortOptions(configOptions);
				tables.add(Tuple2.of(group, toHtmlTable(configOptions)));
			}
			List<OptionWithMetaInfo> configOptions = tree.getDefaultOptions();
			sortOptions(configOptions);
			tables.add(Tuple2.of(null, toHtmlTable(configOptions)));
		} else {
			sortOptions(allOptions);
			tables = Collections.singletonList(Tuple2.of(null, toHtmlTable(allOptions)));
		}
		return tables;
	}

	static List<OptionWithMetaInfo> extractConfigOptions(Class<?> clazz) {
		try {
			List<OptionWithMetaInfo> configOptions = new ArrayList<>(8);
			Field[] fields = clazz.getFields();
			for (Field field : fields) {
				if (field.getType().equals(ConfigOption.class) && field.getAnnotation(Deprecated.class) == null) {
					configOptions.add(new OptionWithMetaInfo((ConfigOption<?>) field.get(null), field));
				}
			}

			return configOptions;
		} catch (Exception e) {
			throw new RuntimeException("Failed to extract config options from class " + clazz + '.', e);
		}
	}

	/**
	 * Transforms this configuration group into HTML formatted table.
	 * Options are sorted alphabetically by key.
	 *
	 * @param options list of options to include in this group
	 * @return string containing HTML formatted table
	 */
	private static String toHtmlTable(final List<OptionWithMetaInfo> options) {
		StringBuilder htmlTable = new StringBuilder();
		htmlTable.append("<table class=\"table table-bordered\">\n");
		htmlTable.append("    <thead>\n");
		htmlTable.append("        <tr>\n");
		htmlTable.append("            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n");
		htmlTable.append("            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n");
		htmlTable.append("            <th class=\"text-left\" style=\"width: 65%\">Description</th>\n");
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

		return "" +
			"        <tr>\n" +
			"            <td><h5>" + escapeCharacters(option.key()) + "</h5></td>\n" +
			"            <td style=\"word-wrap: break-word;\">" + escapeCharacters(addWordBreakOpportunities(defaultValue)) + "</td>\n" +
			"            <td>" + escapeCharacters(option.description()) + "</td>\n" +
			"        </tr>\n";
	}

	static String stringifyDefault(OptionWithMetaInfo optionWithMetaInfo) {
		ConfigOption<?> option = optionWithMetaInfo.option;
		Documentation.OverrideDefault overrideDocumentedDefault = optionWithMetaInfo.field.getAnnotation(Documentation.OverrideDefault.class);
		if (overrideDocumentedDefault != null) {
			return overrideDocumentedDefault.value();
		} else {
			Object value = option.defaultValue();
			if (value instanceof String) {
				if (((String) value).isEmpty()) {
					return "(none)";
				}
				return "\"" + value + "\"";
			}
			return value == null ? "(none)" : value.toString();
		}
	}

	private static String escapeCharacters(String value) {
		return value
			.replaceAll("<wbr>", TEMPORARY_PLACEHOLDER)
			.replaceAll("<", "&#60;")
			.replaceAll(">", "&#62;")
			.replaceAll(TEMPORARY_PLACEHOLDER, "<wbr>");
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
	 * Data structure used to assign {@link ConfigOption ConfigOptions} to the {@link ConfigGroup} with the longest
	 * matching prefix.
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

			// assign options to their corresponding group, i.e. the last group root node encountered when traversing
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
			Node currentNode = root;
			for (String keyComponent : keyComponents) {
				currentNode = currentNode.findChild(keyComponent);
			}
			return currentNode.isGroupRoot() ? currentNode : root;
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

			private Node findChild(String keyComponent) {
				Node child = children.get(keyComponent);
				if (child == null) {
					return this;
				}
				return child;
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

	private ConfigOptionsDocGenerator() {
	}
}
