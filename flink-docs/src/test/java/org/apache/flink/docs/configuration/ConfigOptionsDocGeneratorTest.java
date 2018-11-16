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

import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Formatter;
import org.apache.flink.configuration.description.HtmlFormatter;
import org.apache.flink.docs.configuration.data.TestCommonOptions;
import org.apache.flink.util.FileUtils;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link ConfigOptionsDocGenerator}.
 */
public class ConfigOptionsDocGeneratorTest {

	@ClassRule
	public static final TemporaryFolder TMP = new TemporaryFolder();

	static class TestConfigGroup {
		public static ConfigOption<Integer> firstOption = ConfigOptions
			.key("first.option.a")
			.defaultValue(2)
			.withDescription("This is example description for the first option.");

		public static ConfigOption<String> secondOption = ConfigOptions
			.key("second.option.a")
			.noDefaultValue()
			.withDescription("This is long example description for the second option.");
	}

	@Test
	public void testCreatingDescription() {
		final String expectedTable =
			"<table class=\"table table-bordered\">\n" +
			"    <thead>\n" +
			"        <tr>\n" +
			"            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n" +
			"            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n" +
			"            <th class=\"text-left\" style=\"width: 65%\">Description</th>\n" +
			"        </tr>\n" +
			"    </thead>\n" +
			"    <tbody>\n" +
			"        <tr>\n" +
			"            <td><h5>first.option.a</h5></td>\n" +
			"            <td style=\"word-wrap: break-word;\">2</td>\n" +
			"            <td>This is example description for the first option.</td>\n" +
			"        </tr>\n" +
			"        <tr>\n" +
			"            <td><h5>second.option.a</h5></td>\n" +
			"            <td style=\"word-wrap: break-word;\">(none)</td>\n" +
			"            <td>This is long example description for the second option.</td>\n" +
			"        </tr>\n" +
			"    </tbody>\n" +
			"</table>\n";
		final String htmlTable = ConfigOptionsDocGenerator.generateTablesForClass(TestConfigGroup.class).get(0).f1;

		assertEquals(expectedTable, htmlTable);
	}

	@ConfigGroups(groups = {
		@ConfigGroup(name = "firstGroup", keyPrefix = "first"),
		@ConfigGroup(name = "secondGroup", keyPrefix = "second")})
	static class TestConfigMultipleSubGroup {
		public static ConfigOption<Integer> firstOption = ConfigOptions
			.key("first.option.a")
			.defaultValue(2)
			.withDescription("This is example description for the first option.");

		public static ConfigOption<String> secondOption = ConfigOptions
			.key("second.option.a")
			.noDefaultValue()
			.withDescription("This is long example description for the second option.");

		public static ConfigOption<Integer> thirdOption = ConfigOptions
			.key("third.option.a")
			.defaultValue(2)
			.withDescription("This is example description for the third option.");

		public static ConfigOption<String> fourthOption = ConfigOptions
			.key("fourth.option.a")
			.noDefaultValue()
			.withDescription("This is long example description for the fourth option.");
	}

	@Test
	public void testCreatingMultipleGroups() {
		final List<Tuple2<ConfigGroup, String>> tables = ConfigOptionsDocGenerator.generateTablesForClass(
			TestConfigMultipleSubGroup.class);

		assertEquals(tables.size(), 3);
		final HashMap<String, String> tablesConverted = new HashMap<>();
		for (Tuple2<ConfigGroup, String> table : tables) {
			tablesConverted.put(table.f0 != null ? table.f0.name() : "default", table.f1);
		}

		assertEquals(
			"<table class=\"table table-bordered\">\n" +
			"    <thead>\n" +
			"        <tr>\n" +
			"            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n" +
			"            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n" +
			"            <th class=\"text-left\" style=\"width: 65%\">Description</th>\n" +
			"        </tr>\n" +
			"    </thead>\n" +
			"    <tbody>\n" +
			"        <tr>\n" +
			"            <td><h5>first.option.a</h5></td>\n" +
			"            <td style=\"word-wrap: break-word;\">2</td>\n" +
			"            <td>This is example description for the first option.</td>\n" +
			"        </tr>\n" +
			"    </tbody>\n" +
			"</table>\n", tablesConverted.get("firstGroup"));
		assertEquals(
			"<table class=\"table table-bordered\">\n" +
			"    <thead>\n" +
			"        <tr>\n" +
			"            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n" +
			"            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n" +
			"            <th class=\"text-left\" style=\"width: 65%\">Description</th>\n" +
			"        </tr>\n" +
			"    </thead>\n" +
			"    <tbody>\n" +
			"        <tr>\n" +
			"            <td><h5>second.option.a</h5></td>\n" +
			"            <td style=\"word-wrap: break-word;\">(none)</td>\n" +
			"            <td>This is long example description for the second option.</td>\n" +
			"        </tr>\n" +
			"    </tbody>\n" +
			"</table>\n", tablesConverted.get("secondGroup"));
		assertEquals(
			"<table class=\"table table-bordered\">\n" +
			"    <thead>\n" +
			"        <tr>\n" +
			"            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n" +
			"            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n" +
			"            <th class=\"text-left\" style=\"width: 65%\">Description</th>\n" +
			"        </tr>\n" +
			"    </thead>\n" +
			"    <tbody>\n" +
			"        <tr>\n" +
			"            <td><h5>fourth.option.a</h5></td>\n" +
			"            <td style=\"word-wrap: break-word;\">(none)</td>\n" +
			"            <td>This is long example description for the fourth option.</td>\n" +
			"        </tr>\n" +
			"        <tr>\n" +
			"            <td><h5>third.option.a</h5></td>\n" +
			"            <td style=\"word-wrap: break-word;\">2</td>\n" +
			"            <td>This is example description for the third option.</td>\n" +
			"        </tr>\n" +
			"    </tbody>\n" +
			"</table>\n", tablesConverted.get("default"));
	}

	static class TestConfigGroupWithOverriddenDefault {
		@Documentation.OverrideDefault("default_1")
		public static ConfigOption<Integer> firstOption = ConfigOptions
			.key("first.option.a")
			.defaultValue(2)
			.withDescription("This is example description for the first option.");

		@Documentation.OverrideDefault("default_2")
		public static ConfigOption<String> secondOption = ConfigOptions
			.key("second.option.a")
			.noDefaultValue()
			.withDescription("This is long example description for the second option.");
	}

	@Test
	public void testOverrideDefault() {
		final String expectedTable =
			"<table class=\"table table-bordered\">\n" +
				"    <thead>\n" +
				"        <tr>\n" +
				"            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n" +
				"            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n" +
				"            <th class=\"text-left\" style=\"width: 65%\">Description</th>\n" +
				"        </tr>\n" +
				"    </thead>\n" +
				"    <tbody>\n" +
				"        <tr>\n" +
				"            <td><h5>first.option.a</h5></td>\n" +
				"            <td style=\"word-wrap: break-word;\">default_1</td>\n" +
				"            <td>This is example description for the first option.</td>\n" +
				"        </tr>\n" +
				"        <tr>\n" +
				"            <td><h5>second.option.a</h5></td>\n" +
				"            <td style=\"word-wrap: break-word;\">default_2</td>\n" +
				"            <td>This is long example description for the second option.</td>\n" +
				"        </tr>\n" +
				"    </tbody>\n" +
				"</table>\n";
		final String htmlTable = ConfigOptionsDocGenerator.generateTablesForClass(TestConfigGroupWithOverriddenDefault.class).get(0).f1;

		assertEquals(expectedTable, htmlTable);
	}

	@Test
	public void testCommonOptions() throws IOException, ClassNotFoundException {
		final String projectRootDir = System.getProperty("rootDir");
		final String outputDirectory = TMP.newFolder().getAbsolutePath();

		final OptionsClassLocation[] locations = new OptionsClassLocation[] {
			new OptionsClassLocation("flink-docs", TestCommonOptions.class.getPackage().getName())
		};

		ConfigOptionsDocGenerator.generateCommonSection(projectRootDir, outputDirectory, locations, "src/test/java");
		Formatter formatter = new HtmlFormatter();

		String expected =
			"<table class=\"table table-bordered\">\n" +
			"    <thead>\n" +
			"        <tr>\n" +
			"            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n" +
			"            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n" +
			"            <th class=\"text-left\" style=\"width: 65%\">Description</th>\n" +
			"        </tr>\n" +
			"    </thead>\n" +
			"    <tbody>\n" +
			"        <tr>\n" +
			"            <td><h5>" + TestCommonOptions.COMMON_POSITIONED_OPTION.key() + "</h5></td>\n" +
			"            <td style=\"word-wrap: break-word;\">" + TestCommonOptions.COMMON_POSITIONED_OPTION.defaultValue() + "</td>\n" +
			"            <td>" + formatter.format(TestCommonOptions.COMMON_POSITIONED_OPTION.description()) + "</td>\n" +
			"        </tr>\n" +
			"        <tr>\n" +
			"            <td><h5>" + TestCommonOptions.COMMON_OPTION.key() + "</h5></td>\n" +
			"            <td style=\"word-wrap: break-word;\">" + TestCommonOptions.COMMON_OPTION.defaultValue() + "</td>\n" +
			"            <td>" + formatter.format(TestCommonOptions.COMMON_OPTION.description()) + "</td>\n" +
			"        </tr>\n" +
			"    </tbody>\n" +
			"</table>\n";

		String output = FileUtils.readFile(Paths.get(outputDirectory, ConfigOptionsDocGenerator.COMMON_SECTION_FILE_NAME).toFile(), StandardCharsets.UTF_8.name());

		assertEquals(expected, output);
	}

	static class TestConfigGroupWithExclusion {
		public static ConfigOption<Integer> firstOption = ConfigOptions
			.key("first.option.a")
			.defaultValue(2)
			.withDescription("This is example description for the first option.");

		@Documentation.ExcludeFromDocumentation
		public static ConfigOption<String> excludedOption = ConfigOptions
			.key("excluded.option.a")
			.noDefaultValue()
			.withDescription("This should not be documented.");
	}

	/**
	 * Tests that {@link ConfigOption} annotated with {@link Documentation.ExcludeFromDocumentation}
	 * are not documented.
	 */
	@Test
	public void testConfigOptionExclusion() {
		final String expectedTable =
			"<table class=\"table table-bordered\">\n" +
				"    <thead>\n" +
				"        <tr>\n" +
				"            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n" +
				"            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n" +
				"            <th class=\"text-left\" style=\"width: 65%\">Description</th>\n" +
				"        </tr>\n" +
				"    </thead>\n" +
				"    <tbody>\n" +
				"        <tr>\n" +
				"            <td><h5>first.option.a</h5></td>\n" +
				"            <td style=\"word-wrap: break-word;\">2</td>\n" +
				"            <td>This is example description for the first option.</td>\n" +
				"        </tr>\n" +
				"    </tbody>\n" +
				"</table>\n";
		final String htmlTable = ConfigOptionsDocGenerator.generateTablesForClass(TestConfigGroupWithExclusion.class).get(0).f1;

		assertEquals(expectedTable, htmlTable);
	}
}
