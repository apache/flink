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

import org.junit.Test;

import static org.junit.Assert.*;

public class ConfigOptionsDocGeneratorTest {

	public static class TestConfigGroup {
		public static ConfigOption<Integer> firstOption = ConfigOptions
			.key("first.option.a")
			.defaultValue(2)
			.withDescription("This is example description for the first option.");

		public static ConfigOption<String> secondOption = ConfigOptions
			.key("second.option.a")
			.noDefaultValue()
			.withDescription("Short second option descr",
				"This is long example description for the second option.");

		private TestConfigGroup() {
		}
	}

	@Test
	public void testCreatingNoShortDescription() throws Exception {
		final String expectedTable = "<table class=\"table table-bordered\">" +
			"<thead>" +
			"<tr>" +
			"<th class=\"text-left\" style=\"width: 20%\">Name</th>" +
			"<th class=\"text-left\" style=\"width: 15%\">Default Value</th>" +
			"<th class=\"text-left\" style=\"width: 65%\">Description</th>" +
			"</tr>" +
			"</thead>" +
			"<tbody>" +
			"<tr>" +
			"<td>first.option.a</td>" +
			"<td>2</td>" +
			"<td>This is example description for the first option.</td>" +
			"</tr>" +
			"<tr>" +
			"<td>second.option.a</td>" +
			"<td>(none)</td>" +
			"<td>This is long example description for the second option.</td>" +
			"</tr>" +
			"</tbody>" +
			"</table>";
		final String htmlTable = ConfigOptionsDocGenerator.create(TestConfigGroup.class, false);

		assertEquals(expectedTable, htmlTable);
	}

	@Test
	public void testCreatingWithShortDescription() throws Exception {
		final String expectedTable = "<table class=\"table table-bordered\">" +
		                             "<thead>" +
		                             "<tr>" +
		                             "<th class=\"text-left\" style=\"width: 20%\">Name</th>" +
		                             "<th class=\"text-left\" style=\"width: 15%\">Default Value</th>" +
		                             "<th class=\"text-left\" style=\"width: 25%\">Short description</th>" +
		                             "<th class=\"text-left\" style=\"width: 40%\">Description</th>" +
		                             "</tr>" +
		                             "</thead>" +
		                             "<tbody>" +
		                             "<tr>" +
		                             "<td>first.option.a</td>" +
		                             "<td>2</td>" +
		                             "<td></td>" +
		                             "<td>This is example description for the first option.</td>" +
		                             "</tr>" +
		                             "<tr>" +
		                             "<td>second.option.a</td>" +
		                             "<td>(none)</td>" +
		                             "<td>Short second option descr</td>" +
		                             "<td>This is long example description for the second option.</td>" +
		                             "</tr>" +
		                             "</tbody>" +
		                             "</table>";
		final String htmlTable = ConfigOptionsDocGenerator.create(TestConfigGroup.class, true);

		assertEquals(expectedTable, htmlTable);
	}
}
