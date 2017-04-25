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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class used for generating code based documentation of configuration parameters.
 */
public class ConfigOptionsDocGenerator {

	/**
	 * Transforms this configuration group into HTML formatted table.
	 * Options are sorted alphabetically by key.
	 *
	 * @param options list of options to include in this group
	 * @param includeShort should the short description be included
	 * @return string containing HTML formatted table
	 */
	private static String toHTMLTable(final List<ConfigOption> options, boolean includeShort) {
		final StringBuilder htmlTable = new StringBuilder(
			"<table class=\"table table-bordered\"><thead><tr><th class=\"text-left\" style=\"width: 20%\">Name</th>" +
			"<th class=\"text-left\" style=\"width: 15%\">Default Value</th>");
		if (includeShort) {
			htmlTable.append(
				"<th class=\"text-left\" style=\"width: 25%\">Short description</th>" +
				"<th class=\"text-left\" style=\"width: 40%\">Description</th></tr></thead><tbody>");
		} else {
			htmlTable.append(
				"<th class=\"text-left\" style=\"width: 65%\">Description</th></tr></thead><tbody>");
		}

		for (ConfigOption option : options) {
			htmlTable.append(toHTMLString(option, includeShort));
		}

		htmlTable.append("</tbody></table>");

		return htmlTable.toString();
	}

	/**
	 * Transforms this configuration group into HTML formatted table.
	 * Options are sorted alphabetically by key.
	 *
	 * @param clazzz a class that contains options to be converted int documentation
	 * @param includeShort should the short description be included
	 * @return string containing HTML formatted table
	 */
	static String create(Class<?> clazzz, boolean includeShort) {
		try {
			final List<ConfigOption> configOptions = new ArrayList<>();
			final Field[] fields = clazzz.getFields();
			for (Field field : fields) {
				if (field.getType().equals(ConfigOption.class)) {
					configOptions.add((ConfigOption) field.get(null));
				}
			}

			Collections.sort(configOptions, new Comparator<ConfigOption>() {
				@Override
				public int compare(ConfigOption o1, ConfigOption o2) {
					return o1.key().compareTo(o2.key());
				}
			});

			return toHTMLTable(configOptions, includeShort);
		} catch (Exception e) {
			throw new RuntimeException("Could not retrieve all options.", e);
		}
	}

	/**
	 * Transforms option to table row.
	 *
	 * @param option option to transform
	 * @param includeShort should the short description be included
	 * @return row with the option description
	 */
	private static String toHTMLString(final ConfigOption<?> option, boolean includeShort) {
		final StringBuilder stringBuilder = new StringBuilder("<tr><td>")
			.append(option.key()).append("</td>")
			.append("<td>")
			.append(defaultValueToHtml(option.defaultValue()))
			.append("</td>");
		if (includeShort) {
			stringBuilder.append("<td>").append(option.shortDescription()).append("</td>");
		}

		stringBuilder.append("<td>").append(option.description()).append("</td></tr>");
		return stringBuilder.toString();
	}

	private static String defaultValueToHtml(Object value) {
		if (value instanceof String) {
			return "\"" + value +"\"";
		}

		return value == null ? "(none)" : value.toString();
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

					final String outputFile = matcher.group(2).replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase() +
						"_configuration.html";

					final String className = matcher.group(1);
					final String doc = ConfigOptionsDocGenerator.create(Class.forName(packageName + "." + className), false);
					Files.write(Paths.get(outputPath, outputFile), doc.getBytes(StandardCharsets.UTF_8));
				}
			}
		}
	}


	private ConfigOptionsDocGenerator() {
	}
}
