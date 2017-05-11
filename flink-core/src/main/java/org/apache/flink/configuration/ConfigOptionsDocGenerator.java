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
package org.apache.flink.configuration;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Class used for generating code based documentation of configuration parameters.
 */
public class ConfigOptionsDocGenerator {

	/**
	 * Transforms this configuration group into HTML formatted table.
	 * Options are sorted alphabetically by key.
	 *
	 * @param options list of options to include in this group
	 * @return string containing HTML formatted table
	 */
	private static String toHTMLTable(final List<ConfigOption> options) {
		final StringBuilder htmlTable = new StringBuilder(
			"<table class=\"table table-bordered\"><thead><tr><th class=\"text-left\" style=\"width: 20%\">Key</th>" +
			"<th class=\"text-left\" style=\"width: 15%\">Default Value</th><th class=\"text-left\" " +
			"style=\"width: 65%\">Description</th></tr></thead><tbody>");

		for (ConfigOption option : options) {
			htmlTable.append(toHTMLString(option));
		}

		htmlTable.append("</tbody></table>");

		return htmlTable.toString();
	}

	/**
	 * Transforms this configuration group into HTML formatted table.
	 * Options are sorted alphabetically by key.
	 *
	 * @param clazzz a class that contains options to be converted int documentation
	 * @return string containing HTML formatted table
	 */
	static String create(Class<?> clazzz) {
		final List<ConfigOption> configOptions = extractConfigOptions(clazzz);

		return toHTMLTable(configOptions);
	}

	private static List<ConfigOption> extractConfigOptions(Class<?> clazzz) {
		try {
			final List<ConfigOption> configOptions = new ArrayList<>();
			final Field[] fields = clazzz.getFields();
			for (Field field : fields) {
				if (field.getType().equals(ConfigOption.class) && field.getAnnotation(Deprecated.class) == null) {
					configOptions.add((ConfigOption) field.get(null));
				}
			}

			return configOptions;
		} catch (Exception e) {
			throw new RuntimeException("Could not retrieve all options.", e);
		}
	}

	/**
	 * Transforms option to table row.
	 *
	 * @param option option to transform
	 * @return row with the option description
	 */
	private static String toHTMLString(final ConfigOption<?> option) {
		return "<tr>" +
				"<td>" + option.key() + "</td>" +
				"<td>" + defaultValueToHtml(option.defaultValue()) + "</td>" +
				"<td>" + option.description() + "</td>" +
				"</tr>";
	}

	private static String defaultValueToHtml(Object value) {
		if (value instanceof String) {
			return "\"" + value +"\"";
		}

		return value == null ? "(none)" : value.toString();
	}

	private static void sortOptions(List<ConfigOption> configOptions) {
		Collections.sort(configOptions, new Comparator<ConfigOption>() {
			@Override
			public int compare(ConfigOption o1, ConfigOption o2) {
				return o1.key().compareTo(o2.key());
			}
		});
	}

	static List<Tuple2<ConfigGroup, String>> generateTablesForClass(Class<?> optionsClass) {
		final ConfigGroups configGroups = optionsClass.getAnnotation(ConfigGroups.class);
		List<Tuple2<ConfigGroup, String>> tables = new ArrayList<>();
		final List<ConfigOption> allOptions = extractConfigOptions(optionsClass);

		if (configGroups != null) {
			final Tree tree = new Tree();

			for (ConfigGroup group : configGroups.groups()) {
				tree.set(group);
			}

			for (ConfigOption configOption : allOptions) {
				tree.add(configOption);
			}

			for (ConfigGroup group : configGroups.groups()) {
				List<ConfigOption> configOptions = tree.findConfigOptions(group);
				sortOptions(configOptions);
				tables.add(Tuple2.of(group, toHTMLTable(configOptions)));
			}
			List<ConfigOption> configOptions = tree.getDefaultOptions();
			sortOptions(configOptions);
			tables.add(Tuple2.<ConfigGroup, String>of(null, toHTMLTable(configOptions)));
		} else {
			sortOptions(allOptions);
			tables.add(Tuple2.<ConfigGroup, String>of(null, toHTMLTable(allOptions)));
		}
		return tables;
	}

	/**
	 * Method used to generated documentation entries containing tables of configuration options with default value and
	 * description. Each found class matching a pattern *Options.java will results in a separate file with a configuration
	 * table.
	 *
	 * @param args first argument is output path for the generated files, second argument is full package name containing
	 *             classes with {@link ConfigOption}
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		final String outputPath = args[0];
		final String packageName = args[1];

		final Path configDir = Paths.get("../src/main/java", packageName.replaceAll("\\.", "/"));

		final Pattern p = Pattern.compile("(([a-zA-Z]*)(Options))\\.java");
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(configDir, "*Options.java")) {
			for (Path entry: stream) {
				final String fileName = entry.getFileName().toString();
				final Matcher matcher = p.matcher(fileName);
				if (!fileName.equals("ConfigOptions.java") && matcher.matches()) {
					final Class<?> optionsClass = Class.forName(packageName + "." + matcher.group(1));
					List<Tuple2<ConfigGroup, String>> tables = generateTablesForClass(optionsClass);
					for (Tuple2<ConfigGroup, String> group : tables) {

						String name = group.f0 == null
							? matcher.group(2).replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase()
							: group.f0.name().replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();

						final String outputFile = name + "_configuration.html";
						Files.write(Paths.get(outputPath, outputFile), group.f1.getBytes(StandardCharsets.UTF_8));
					}
				}
			}
		}
	}

	private static class Tree {
		private final Node root = new Node();

		private final Node defaultGroup = new Node();

		public Tree() {
		}

		private static class Node {
			private final List<ConfigOption> configOptions = new ArrayList<>();
			private final Map<String, Node> children = new HashMap<>();
			private boolean terminal = false;

			Node addChild(String keyComponent) {
				Node child = children.get(keyComponent);
				if (child == null) {
					child = new Node();
					children.put(keyComponent, child);
				}
				return child;
			}

			Node findChild(String keyComponent) {
				Node child = children.get(keyComponent);
				if (child == null) {
					return this;
				}
				return child;
			}

			void addOption(ConfigOption option) {
				configOptions.add(option);
			}

			public boolean isTerminal() {
				return terminal;
			}

			public void setTerminal() {
				this.terminal = true;
			}

			public List<ConfigOption> getConfigOptions() {
				return configOptions;
			}
		}

		public void add(ConfigOption<?> option) {
			findNode(option.key())
				.addOption(option);
		}

		public void set(ConfigGroup group) {
			addNode(group.keyPrefix());
		}

		private Node addNode(String key) {
			String[] keyComponents = key.split("\\.");
			Node currentNode = root;
			for (String keyComponent : keyComponents) {
				currentNode = currentNode.addChild(keyComponent);
			}

			currentNode.setTerminal();
			return currentNode;
		}

		private Node findNode(String key) {
			String[] keyComponents = key.split("\\.");
			Node currentNode = root;
			for (String keyComponent : keyComponents) {
				currentNode = currentNode.findChild(keyComponent);
			}
			return currentNode.isTerminal() ? currentNode : defaultGroup;
		}

		public List<ConfigOption> findConfigOptions(ConfigGroup configGroup) {
			Node subtreeRoot = findNode(configGroup.keyPrefix());
			return subtreeRoot.getConfigOptions();
		}

		public List<ConfigOption> getDefaultOptions() {
			return defaultGroup.getConfigOptions();
		}
	}

	private ConfigOptionsDocGenerator() {
	}
}
