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

package org.apache.flink.table.api.config;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by Flink's table module.
 *
 * <p>This is only used for the Blink planner.
 *
 * <p>NOTE: All option keys in this class must start with "table.parser".
 */
public class ParserConfigOptions {

	// ------------------------------------------------------------------------
	//  Parser Options
	// ------------------------------------------------------------------------
	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> TABLE_PARSER_CASE_SENSITIVE_ENABLED =
		key("table.parser.case-sensitive-enabled")
			.defaultValue(true)
			.withDescription("When it is true, the identifiers are matched case-sensitively.");
}
