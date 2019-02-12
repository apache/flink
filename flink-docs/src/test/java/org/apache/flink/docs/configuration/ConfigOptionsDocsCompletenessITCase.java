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

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.Formatter;
import org.apache.flink.configuration.description.HtmlFormatter;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.docs.configuration.ConfigOptionsDocGenerator.COMMON_SECTION_FILE_NAME;
import static org.apache.flink.docs.configuration.ConfigOptionsDocGenerator.DEFAULT_PATH_PREFIX;
import static org.apache.flink.docs.configuration.ConfigOptionsDocGenerator.LOCATIONS;
import static org.apache.flink.docs.configuration.ConfigOptionsDocGenerator.extractConfigOptions;
import static org.apache.flink.docs.configuration.ConfigOptionsDocGenerator.processConfigOptions;
import static org.apache.flink.docs.configuration.ConfigOptionsDocGenerator.stringifyDefault;

/**
 * This test verifies that all {@link ConfigOption ConfigOptions} in the configured
 * {@link ConfigOptionsDocGenerator#LOCATIONS locations} are documented and well-defined (i.e. no 2 options exist for
 * the same key with different descriptions/default values), and that the documentation does not refer to non-existent
 * options.
 */
public class ConfigOptionsDocsCompletenessITCase {

	private static final Formatter htmlFormatter = new HtmlFormatter();

	@Test
	public void testCommonSectionCompleteness() throws IOException, ClassNotFoundException {
		Map<String, DocumentedOption> documentedOptions = parseDocumentedCommonOptions();
		Map<String, ExistingOption> existingOptions = findExistingOptions(
			optionWithMetaInfo -> optionWithMetaInfo.field.getAnnotation(Documentation.CommonOption.class) != null);

		compareDocumentedAndExistingOptions(documentedOptions, existingOptions);
	}

	@Test
	public void testFullReferenceCompleteness() throws IOException, ClassNotFoundException {
		Map<String, DocumentedOption> documentedOptions = parseDocumentedOptions();
		Map<String, ExistingOption> existingOptions = findExistingOptions(ignored -> true);

		compareDocumentedAndExistingOptions(documentedOptions, existingOptions);
	}

	private static void compareDocumentedAndExistingOptions(Map<String, DocumentedOption> documentedOptions, Map<String, ExistingOption> existingOptions) {
		final Collection<String> problems = new ArrayList<>(0);

		// first check that all existing options are properly documented
		existingOptions.forEach((key, supposedState) -> {
			DocumentedOption documentedState = documentedOptions.remove(key);

			// if nothing matches the docs for this option are up-to-date
			if (documentedState == null) {
				// option is not documented at all
				problems.add("Option " + supposedState.key + " in " + supposedState.containingClass + " is not documented.");
			} else if (!supposedState.defaultValue.equals(documentedState.defaultValue)) {
				// default is outdated
				problems.add("Documented default of " + supposedState.key + " in " + supposedState.containingClass +
					" is outdated. Expected: " + supposedState.defaultValue + " Actual: " + documentedState.defaultValue);
			} else if (!supposedState.description.equals(documentedState.description)) {
				// description is outdated
				problems.add("Documented description of " + supposedState.key + " in " + supposedState.containingClass +
					" is outdated.");
			}
		});

		// documentation contains an option that no longer exists
		if (!documentedOptions.isEmpty()) {
			for (DocumentedOption documentedOption : documentedOptions.values()) {
				problems.add("Documented option " + documentedOption.key + " does not exist.");
			}
		}

		if (!problems.isEmpty()) {
			StringBuilder sb = new StringBuilder("Documentation is outdated, please regenerate it according to the" +
				" instructions in flink-docs/README.md.");
			sb.append(System.lineSeparator());
			sb.append("\tProblems:");
			for (String problem : problems) {
				sb.append(System.lineSeparator());
				sb.append("\t\t");
				sb.append(problem);
			}
			Assert.fail(sb.toString());
		}
	}

	private static Map<String, DocumentedOption> parseDocumentedCommonOptions() throws IOException {
		Path commonSection = Paths.get(System.getProperty("rootDir"), "docs", "_includes", "generated", COMMON_SECTION_FILE_NAME);
		return parseDocumentedOptionsFromFile(commonSection).stream()
			.collect(Collectors.toMap(option -> option.key, option -> option, (option1, option2) -> {
				if (option1.equals(option2)) {
					// we allow multiple instances of ConfigOptions with the same key if they are identical
					return option1;
				} else {
					// found a ConfigOption pair with the same key that aren't equal
					// we fail here outright as this is not a documentation-completeness problem
					if (!option1.defaultValue.equals(option2.defaultValue)) {
						throw new AssertionError("Documentation contains distinct defaults for " +
							option1.key + " in " + option1.containingFile + " and " + option2.containingFile + '.');
					} else {
						throw new AssertionError("Documentation contains distinct descriptions for " +
							option1.key + " in " + option1.containingFile + " and " + option2.containingFile + '.');
					}
				}
			}));
	}

	private static Map<String, DocumentedOption> parseDocumentedOptions() throws IOException {
		Path includeFolder = Paths.get(System.getProperty("rootDir"), "docs", "_includes", "generated").toAbsolutePath();
		return Files.list(includeFolder)
			.filter(path -> path.getFileName().toString().contains("configuration"))
			.flatMap(file -> {
				try {
					return parseDocumentedOptionsFromFile(file).stream();
				} catch (IOException ignored) {
					return Stream.empty();
				}
			})
			.collect(Collectors.toMap(option -> option.key, option -> option, (option1, option2) -> {
				if (option1.equals(option2)) {
					// we allow multiple instances of ConfigOptions with the same key if they are identical
					return option1;
				} else {
					// found a ConfigOption pair with the same key that aren't equal
					// we fail here outright as this is not a documentation-completeness problem
					if (!option1.defaultValue.equals(option2.defaultValue)) {
						throw new AssertionError("Documentation contains distinct defaults for " +
							option1.key + " in " + option1.containingFile + " and " + option2.containingFile + '.');
					} else {
						throw new AssertionError("Documentation contains distinct descriptions for " +
							option1.key + " in " + option1.containingFile + " and " + option2.containingFile + '.');
					}
				}
			}));
	}

	private static Collection<DocumentedOption> parseDocumentedOptionsFromFile(Path file) throws IOException {
		Document document = Jsoup.parse(file.toFile(), StandardCharsets.UTF_8.name());
		document.outputSettings().syntax(Document.OutputSettings.Syntax.xml);
		document.outputSettings().prettyPrint(false);
		return document.getElementsByTag("table").stream()
			.map(element -> element.getElementsByTag("tbody").get(0))
			.flatMap(element -> element.getElementsByTag("tr").stream())
			.map(tableRow -> {
				String key = tableRow.child(0).text();
				String defaultValue = tableRow.child(1).text();
				String description = tableRow.child(2)
					.childNodes()
					.stream()
					.map(Object::toString)
					.collect(Collectors.joining());
				return new DocumentedOption(key, defaultValue, description, file.getName(file.getNameCount() - 1));
			})
			.collect(Collectors.toList());
	}

	private static Map<String, ExistingOption> findExistingOptions(Predicate<ConfigOptionsDocGenerator.OptionWithMetaInfo> predicate) throws IOException, ClassNotFoundException {
		Map<String, ExistingOption> existingOptions = new HashMap<>(32);

		for (OptionsClassLocation location : LOCATIONS) {
			processConfigOptions(System.getProperty("rootDir"), location.getModule(), location.getPackage(), DEFAULT_PATH_PREFIX, optionsClass -> {
				List<ConfigOptionsDocGenerator.OptionWithMetaInfo> configOptions = extractConfigOptions(optionsClass);
				for (ConfigOptionsDocGenerator.OptionWithMetaInfo option : configOptions) {
					if (predicate.test(option)) {
						String key = option.option.key();
						String defaultValue = stringifyDefault(option);
						String description = htmlFormatter.format(option.option.description());
						ExistingOption duplicate = existingOptions.put(key, new ExistingOption(key, defaultValue, description, optionsClass));
						if (duplicate != null) {
							// multiple documented options have the same key
							// we fail here outright as this is not a documentation-completeness problem
							if (!(duplicate.description.equals(description))) {
								throw new AssertionError("Ambiguous option " + key + " due to distinct descriptions.");
							} else if (!duplicate.defaultValue.equals(defaultValue)) {
								throw new AssertionError("Ambiguous option " + key + " due to distinct default values (" + defaultValue + " vs " + duplicate.defaultValue + ").");
							}
						}
					}
				}
			});
		}

		return existingOptions;
	}

	private static final class ExistingOption extends Option {

		private final Class<?> containingClass;

		private ExistingOption(String key, String defaultValue, String description, Class<?> containingClass) {
			super(key, defaultValue, description);
			this.containingClass = containingClass;
		}
	}

	private static final class DocumentedOption extends Option {

		private final Path containingFile;

		private DocumentedOption(String key, String defaultValue, String description, Path containingFile) {
			super(key, defaultValue, description);
			this.containingFile = containingFile;
		}
	}

	private abstract static class Option {
		protected final String key;
		protected final String defaultValue;
		protected final String description;

		private Option(String key, String defaultValue, String description) {
			this.key = key;
			this.defaultValue = defaultValue;
			this.description = description;
		}

		@Override
		public int hashCode() {
			return key.hashCode() + defaultValue.hashCode() + description.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof Option)) {
				return false;
			}

			Option other = (Option) obj;

			return this.key.equals(other.key)
				&& this.defaultValue.equals(other.defaultValue)
				&& this.description.equals(other.description);
		}

		@Override
		public String toString() {
			return "Option(key=" + key + ", default=" + defaultValue + ", description=" + description + ')';
		}
	}
}
