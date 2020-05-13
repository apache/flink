/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.csv;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options for CSV format. */
public class CsvOptions {
	private CsvOptions() {}

	// ------------------------------------------------------------------------
	//  Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<String> FIELD_DELIMITER = ConfigOptions
			.key("field-delimiter")
			.stringType()
			.defaultValue(",");

	public static final ConfigOption<String> LINE_DELIMITER = ConfigOptions
			.key("line-delimiter")
			.stringType()
			.defaultValue("\n");

	public static final ConfigOption<Boolean> DISABLE_QUOTE_CHARACTER = ConfigOptions
			.key("disable-quote-character")
			.booleanType()
			.defaultValue(false);

	public static final ConfigOption<String> QUOTE_CHARACTER = ConfigOptions
			.key("quote-character")
			.stringType()
			.defaultValue("\"");

	public static final ConfigOption<Boolean> ALLOW_COMMENTS = ConfigOptions
			.key("allow-comments")
			.booleanType()
			.defaultValue(false);

	public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions
			.key("ignore-parse-errors")
			.booleanType()
			.defaultValue(false);

	public static final ConfigOption<String> ARRAY_ELEMENT_DELIMITER = ConfigOptions
			.key("array-element-delimiter")
			.stringType()
			.noDefaultValue();

	public static final ConfigOption<String> ESCAPE_CHARACTER = ConfigOptions
			.key("escape-character")
			.stringType()
			.noDefaultValue();

	public static final ConfigOption<String> NULL_LITERAL = ConfigOptions
			.key("null-literal")
			.stringType()
			.noDefaultValue();
}
