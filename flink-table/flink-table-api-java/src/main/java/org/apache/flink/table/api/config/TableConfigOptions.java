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

package org.apache.flink.table.api.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.SqlDialect;

import java.time.ZoneId;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds {@link org.apache.flink.configuration.ConfigOption}s used by
 * table planner.
 *
 * <p>NOTE: All option keys in this class must start with "table".
 */
@PublicEvolving
public class TableConfigOptions {
	private TableConfigOptions() {}

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED =
			key("table.dynamic-table-options.enabled")
					.booleanType()
					.defaultValue(false)
					.withDescription("Enable or disable the OPTIONS hint used to specify table options" +
							"dynamically, if disabled, an exception would be thrown " +
							"if any OPTIONS hint is specified");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<String> TABLE_SQL_DIALECT = key("table.sql-dialect")
			.stringType()
			.defaultValue(SqlDialect.DEFAULT.name().toLowerCase())
			.withDescription("The SQL dialect defines how to parse a SQL query. " +
					"A different SQL dialect may support different SQL grammar. " +
					"Currently supported dialects are: default and hive");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<String> LOCAL_TIME_ZONE = key("table.local-time-zone")
			.stringType()
			.defaultValue(ZoneId.systemDefault().toString())
			.withDescription("The local time zone defines current session time zone id. It is used when converting to/from " +
				"TIMESTAMP_WITH_LOCAL_TIME_ZONE. Internally, timestamps with local time zone are always represented in the UTC time zone. " +
				"However, when converting to data types that don't include a time zone (e.g. TIMESTAMP, TIME, or simply STRING), " +
				"the session time zone is used during conversion.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Integer> MAX_LENGTH_GENERATED_CODE =
		key("table.generated-code.max-length")
			.intType()
			.defaultValue(64000)
			.withDescription("Specifies a threshold where generated code will be split into sub-function calls. " +
					"Java has a maximum method length of 64 KB. This setting allows for finer granularity if necessary.");

}
