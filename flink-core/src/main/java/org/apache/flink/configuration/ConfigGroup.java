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

	private final List<ConfigOption> options;

	/**
	 * Transforms this configuration group into HTML formatted table.
	 * Options are sorted alphabetically by key.
	 *
	 * @param includeShort should the short description be included
	 * @return string containing HTML formatted table
	 */
	public String toHTMLTable(boolean includeShort) {
		final StringBuilder htmlTable = new StringBuilder(
			"<table><thead><tr><th class=\"text-left\" style=\"width: 20%\">Name</th>" +
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

	static ConfigGroup create(Class<?> clazzz) {
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

			return new ConfigGroup(configOptions);
		} catch (Exception e) {
			throw new RuntimeException("Could not retrieve all options.", e);
		}
	}

	private ConfigGroup(List<ConfigOption> options) {
		this.options = options;
	}


}
