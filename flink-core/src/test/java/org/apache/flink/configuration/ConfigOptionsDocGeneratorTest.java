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

import java.util.HashMap;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import static org.junit.Assert.*;

public class ConfigOptionsDocGeneratorTest {

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
	public void testCreatingDescription() throws Exception {
		final String expectedTable = "<table class=\"table table-bordered\">" +
			"<thead>" +
			"<tr>" +
			"<th class=\"text-left\" style=\"width: 20%\">Key</th>" +
			"<th class=\"text-left\" style=\"width: 15%\">Default Value</th>" +
			"<th class=\"text-left\" style=\"width: 65%\">Description</th>" +
			"</tr>" +
			"</thead>" +
			"<tbody>" +
			"<tr>" +
			"<td><h5>first.option.a</h5></td>" +
			"<td>2</td>" +
			"<td>This is example description for the first option.</td>" +
			"</tr>" +
			"<tr>" +
			"<td><h5>second.option.a</h5></td>" +
			"<td>(none)</td>" +
			"<td>This is long example description for the second option.</td>" +
			"</tr>" +
			"</tbody>" +
			"</table>";
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
	public void testCreatingMultipleGroups() throws Exception {
		final List<Tuple2<ConfigGroup, String>> tables = ConfigOptionsDocGenerator.generateTablesForClass(
			TestConfigMultipleSubGroup.class);

		assertEquals(tables.size(), 3);
		final HashMap<String, String> tablesConverted = new HashMap<>();
		for (Tuple2<ConfigGroup, String> table : tables) {
			tablesConverted.put(table.f0 != null ? table.f0.name() : "default", table.f1);
		}

		assertEquals("<table class=\"table table-bordered\">" +
		             "<thead>" +
		             "<tr>" +
		             "<th class=\"text-left\" style=\"width: 20%\">Key</th>" +
		             "<th class=\"text-left\" style=\"width: 15%\">Default Value</th>" +
		             "<th class=\"text-left\" style=\"width: 65%\">Description</th>" +
		             "</tr>" +
		             "</thead>" +
		             "<tbody>" +
		             "<tr>" +
		             "<td><h5>first.option.a</h5></td>" +
		             "<td>2</td>" +
		             "<td>This is example description for the first option.</td>" +
		             "</tr>" +
		             "</tbody>" +
		             "</table>", tablesConverted.get("firstGroup"));
		assertEquals("<table class=\"table table-bordered\">" +
		             "<thead>" +
		             "<tr>" +
		             "<th class=\"text-left\" style=\"width: 20%\">Key</th>" +
		             "<th class=\"text-left\" style=\"width: 15%\">Default Value</th>" +
		             "<th class=\"text-left\" style=\"width: 65%\">Description</th>" +
		             "</tr>" +
		             "</thead>" +
		             "<tbody>" +
		             "<tr>" +
		             "<td><h5>second.option.a</h5></td>" +
		             "<td>(none)</td>" +
		             "<td>This is long example description for the second option.</td>" +
		             "</tr>" +
		             "</tbody>" +
		             "</table>", tablesConverted.get("secondGroup"));
		assertEquals("<table class=\"table table-bordered\">" +
		             "<thead>" +
		             "<tr>" +
		             "<th class=\"text-left\" style=\"width: 20%\">Key</th>" +
		             "<th class=\"text-left\" style=\"width: 15%\">Default Value</th>" +
		             "<th class=\"text-left\" style=\"width: 65%\">Description</th>" +
		             "</tr>" +
		             "</thead>" +
		             "<tbody>" +
		             "<tr>" +
		             "<td><h5>fourth.option.a</h5></td>" +
		             "<td>(none)</td>" +
		             "<td>This is long example description for the fourth option.</td>" +
		             "</tr>" +
		             "<tr>" +
		             "<td><h5>third.option.a</h5></td>" +
		             "<td>2</td>" +
		             "<td>This is example description for the third option.</td>" +
		             "</tr>" +
		             "</tbody>" +
		             "</table>", tablesConverted.get("default"));
	}

}
