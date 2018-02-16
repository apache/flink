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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigGroup;
import org.apache.flink.configuration.ConfigGroups;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.WebOptions;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
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
	 *  [x] module containing an *Options class
	 *  [x+1] package to the * Options.classes
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		String outputDirectory = args[0];
		String rootDir = args[1];
		for (int x = 2; x + 1 < args.length; x += 2) {
			createTable(rootDir, args[x], args[x + 1], outputDirectory);
		}
	}

	private static void createTable(String rootDir, String module, String packageName, String outputDirectory) throws IOException, ClassNotFoundException {
		Path configDir = Paths.get(rootDir, module, "src/main/java", packageName.replaceAll("\\.", "/"));

		Pattern p = Pattern.compile("(([a-zA-Z]*)(Options|Config|Parameters))\\.java");
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(configDir)) {
			for (Path entry : stream) {
				String fileName = entry.getFileName().toString();
				Matcher matcher = p.matcher(fileName);
				if (!fileName.equals("ConfigOptions.java") && matcher.matches()) {
					Class<?> optionsClass = Class.forName(packageName + "." + matcher.group(1));
					List<Tuple2<ConfigGroup, String>> tables = generateTablesForClass(optionsClass);
					if (tables.size() > 0) {
						for (Tuple2<ConfigGroup, String> group : tables) {

							String name = group.f0 == null
								? matcher.group(2).replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase()
								: group.f0.name().replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();

							String outputFile = name + "_configuration.html";
							Files.write(Paths.get(outputDirectory, outputFile), group.f1.getBytes(StandardCharsets.UTF_8));
						}
					}
				}
			}
		}
	}

	@VisibleForTesting
	static List<Tuple2<ConfigGroup, String>> generateTablesForClass(Class<?> optionsClass) {
		ConfigGroups configGroups = optionsClass.getAnnotation(ConfigGroups.class);
		List<Tuple2<ConfigGroup, String>> tables = new ArrayList<>();
		List<ConfigOption> allOptions = extractConfigOptions(optionsClass);

		if (configGroups != null) {
			Tree tree = new Tree(configGroups.groups(), allOptions);

			for (ConfigGroup group : configGroups.groups()) {
				List<ConfigOption> configOptions = tree.findConfigOptions(group);
				sortOptions(configOptions);
				tables.add(Tuple2.of(group, toHtmlTable(configOptions)));
			}
			List<ConfigOption> configOptions = tree.getDefaultOptions();
			sortOptions(configOptions);
			tables.add(Tuple2.of(null, toHtmlTable(configOptions)));
		} else {
			sortOptions(allOptions);
			tables.add(Tuple2.of(null, toHtmlTable(allOptions)));
		}
		return tables;
	}

	private static List<ConfigOption> extractConfigOptions(Class<?> clazz) {
		try {
			List<ConfigOption> configOptions = new ArrayList<>();
			Field[] fields = clazz.getFields();
			for (Field field : fields) {
				if (field.getType().equals(ConfigOption.class) && field.getAnnotation(Deprecated.class) == null) {
					configOptions.add((ConfigOption) field.get(null));
				}
			}

			return configOptions;
		} catch (Exception e) {
			throw new RuntimeException("Failed to extract config options from class " + clazz + ".", e);
		}
	}

	/**
	 * Transforms this configuration group into HTML formatted table.
	 * Options are sorted alphabetically by key.
	 *
	 * @param options list of options to include in this group
	 * @return string containing HTML formatted table
	 */
	private static String toHtmlTable(final List<ConfigOption> options) {
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

		for (ConfigOption option : options) {
			htmlTable.append(toHtmlString(option));
		}

		htmlTable.append("    </tbody>\n");
		htmlTable.append("</table>\n");

		return htmlTable.toString();
	}

	/**
	 * Transforms option to table row.
	 *
	 * @param option option to transform
	 * @return row with the option description
	 */
	private static String toHtmlString(final ConfigOption<?> option) {
		Object defaultValue = option.defaultValue();
		// This is a temporary hack that should be removed once FLINK-6490 is resolved.
		// These options use System.getProperty("java.io.tmpdir") as the default.
		// As a result the generated table contains an actual path as the default, which is simply wrong.
		if (option == WebOptions.TMP_DIR || option.key().equals("python.dc.tmp.dir") || option == CoreOptions.TMP_DIRS) {
			defaultValue = null;
		}
		return "" +
			"        <tr>\n" +
			"            <td><h5>" + escapeCharacters(option.key()) + "</h5></td>\n" +
			"            <td style=\"word-wrap: break-word;\">" + escapeCharacters(defaultValueToHtml(defaultValue)) + "</td>\n" +
			"            <td>" + escapeCharacters(option.description()) + "</td>\n" +
			"        </tr>\n";
	}

	private static String defaultValueToHtml(Object value) {
		if (value instanceof String) {
			if (((String) value).isEmpty()) {
				return "(none)";
			}
			return "\"" + value + "\"";
		}

		return value == null ? "(none)" : value.toString();
	}

	private static String escapeCharacters(String value) {
		return value
			.replaceAll("<", "&#60;")
			.replaceAll(">", "&#62;");
	}

	private static void sortOptions(List<ConfigOption> configOptions) {
		configOptions.sort(Comparator.comparing(ConfigOption::key));
	}

	/**
	 * Data structure used to assign {@link ConfigOption ConfigOptions} to the {@link ConfigGroup} with the longest
	 * matching prefix.
	 */
	private static class Tree {
		private final Node root = new Node();

		Tree(ConfigGroup[] groups, Collection<ConfigOption> options) {
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
			for (ConfigOption<?> option : options) {
				findGroupRoot(option.key()).assignOption(option);
			}
		}

		List<ConfigOption> findConfigOptions(ConfigGroup configGroup) {
			Node groupRoot = findGroupRoot(configGroup.keyPrefix());
			return groupRoot.getConfigOptions();
		}

		List<ConfigOption> getDefaultOptions() {
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
			private final List<ConfigOption> configOptions = new ArrayList<>();
			private final Map<String, Node> children = new HashMap<>();
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

			private void assignOption(ConfigOption option) {
				configOptions.add(option);
			}

			private boolean isGroupRoot() {
				return isGroupRoot;
			}

			private void markAsGroupRoot() {
				this.isGroupRoot = true;
			}

			private List<ConfigOption> getConfigOptions() {
				return configOptions;
			}
		}
	}

	private ConfigOptionsDocGenerator() {
	}
}
