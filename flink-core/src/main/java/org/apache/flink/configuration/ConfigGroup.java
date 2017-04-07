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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Class used for generating code based documentation of configuration parameters.
 */
public class ConfigGroup {

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
			"<th class=\"text-left\" style=\"width: 20%\">Default Value</th>");
		if (includeShort) {
			htmlTable.append(
				"<th class=\"text-left\" style=\"width: 25%\">Short description</th>" +
				"<th class=\"text-left\" style=\"width: 35%\">Description</th></tr></thead><tbody>");
		} else {
			htmlTable.append(
				"<th class=\"text-left\" style=\"width: 60%\">Description</th></tr></thead><tbody>");
		}

		for (ConfigOption option : options) {
			htmlTable.append(option.toHTMLString(includeShort));
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


	private ConfigGroup() {
	}
}
