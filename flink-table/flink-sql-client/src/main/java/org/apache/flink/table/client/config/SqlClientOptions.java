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

package org.apache.flink.table.client.config;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options used in sql client. */
public class SqlClientOptions {
    private SqlClientOptions() {}

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Integer> EXECUTION_MAX_TABLE_RESULT_ROWS =
            ConfigOptions.key("sql-client.execution.max-table-result.rows")
                    .intType()
                    .defaultValue(1000_000)
                    .withDescription(
                            "The number of rows to cache when in the table mode. If the number of rows exceeds the "
                                    + "specified value, it retries the row in the FIFO style.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<ResultMode> EXECUTION_RESULT_MODE =
            ConfigOptions.key("sql-client.execution.result-mode")
                    .enumType(ResultMode.class)
                    .defaultValue(ResultMode.TABLE)
                    .withDescription(
                            "Determine the mode when display the query result. The available values are ['table', 'tableau', 'changelog']. "
                                    + "The 'table' mode materializes results in memory and visualizes them in a regular, paginated table representation. "
                                    + "The 'changelog' mode does not materialize results and visualizes the result stream that is produced by a continuous query. "
                                    + "The 'tableau' mode is more like a traditional way which will display the results in the screen directly with a tableau format. ");
}
