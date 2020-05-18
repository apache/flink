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

package org.apache.flink.formats.json;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * This class holds configuration constants used by json format.
 */
public class JsonOptions {

	public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD = ConfigOptions
			.key("fail-on-missing-field")
			.booleanType()
			.defaultValue(false)
			.withDescription("Optional flag to specify whether to fail if a field is missing or not, false by default");

	public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions
			.key("ignore-parse-errors")
			.booleanType()
			.defaultValue(false)
			.withDescription("Optional flag to skip fields and rows with parse errors instead of failing;\n"
					+ "fields are set to null in case of errors, false by default");
}
