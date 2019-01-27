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

package org.apache.flink.table.factories.csv;

import org.apache.flink.configuration.ConfigOption;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Options for creating configured instance of CsvTableSource/CsvTableSink.
 */
public class CsvOptions {
	public static final ConfigOption<String> PATH = key("path".toLowerCase())
		.noDefaultValue();

	public static final ConfigOption<Boolean> OPTIONAL_ENUMERATE_NESTED_FILES =
		key("enumerateNestedFiles".toLowerCase()).defaultValue(true);

	public static final ConfigOption<String> OPTIONAL_FIELD_DELIM = key("fieldDelim".toLowerCase())
		.defaultValue(",");

	public static final ConfigOption<String> OPTIONAL_LINE_DELIM = key("lineDelim".toLowerCase())
		.defaultValue("\n");

	public static final ConfigOption<String> OPTIONAL_CHARSET = key("charset".toLowerCase())
		.defaultValue("UTF-8");

	public static final ConfigOption<String> OPTIONAL_WRITE_MODE = key("writeMode".toLowerCase())
		.defaultValue("NO_OVERWRITE");

	public static final ConfigOption<Boolean> OPTIONAL_OVER_RIDE_MODE = key("override".toLowerCase())
		.defaultValue(true);

	public static final ConfigOption<Boolean> EMPTY_COLUMN_AS_NULL = key("emptyColumnAsNull".toLowerCase())
		.defaultValue(false);

	public static final ConfigOption<String> OPTIONAL_QUOTE_CHARACTER = key("quoteCharacter".toLowerCase())
		.noDefaultValue();

	public static final ConfigOption<Boolean> OPTIONAL_FIRST_LINE_AS_HEADER = key("firstLineAsHeader".toLowerCase())
		.defaultValue(false);

	public static final ConfigOption<Integer> PARALLELISM = key("parallelism".toLowerCase())
		.defaultValue(-1);

	public static final ConfigOption<String> OPTIONAL_TIME_ZONE = key("timeZone".toLowerCase()).noDefaultValue();

	public static final ConfigOption<String> OPTIONAL_COMMENTS_PREFIX = key("commentsPrefix".toLowerCase()).noDefaultValue();

	// update mode for the CSV sink, enum: append/upsert/retract, default to be append mode.
	// Notes:
	// 1. append mode can be used for both batch and stream env
	// 2. retract and upsert mode can only used in stream env
	public static final ConfigOption<String> OPTIONAL_UPDATE_MODE = key("updateMode".toLowerCase()).defaultValue("append");

	public static final List<String> SUPPORTED_KEYS = Arrays.asList(
		PATH.key(),
		OPTIONAL_ENUMERATE_NESTED_FILES.key(),
		OPTIONAL_FIELD_DELIM.key(),
		OPTIONAL_LINE_DELIM.key(),
		OPTIONAL_CHARSET.key(),
		OPTIONAL_WRITE_MODE.key(),
		OPTIONAL_OVER_RIDE_MODE.key(),
		EMPTY_COLUMN_AS_NULL.key(),
		OPTIONAL_QUOTE_CHARACTER.key(),
		OPTIONAL_FIRST_LINE_AS_HEADER.key(),
		PARALLELISM.key(),
		OPTIONAL_TIME_ZONE.key(),
		OPTIONAL_COMMENTS_PREFIX.key(),
		OPTIONAL_UPDATE_MODE.key());

	public static final String PARAMS_HELP_MSG = String.format("required params:%s", PATH);
}
