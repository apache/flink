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

package org.apache.flink.table.client.gateway;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.config.ResultMode;
import org.apache.flink.table.client.gateway.result.ChangelogCollectResult;
import org.apache.flink.table.client.gateway.result.DynamicResult;
import org.apache.flink.table.client.gateway.result.MaterializedCollectBatchResult;
import org.apache.flink.table.client.gateway.result.MaterializedCollectStreamResult;
import org.apache.flink.table.utils.print.RowDataToStringConverter;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.client.config.ResultMode.CHANGELOG;
import static org.apache.flink.table.client.config.ResultMode.TABLE;
import static org.apache.flink.table.client.config.SqlClientOptions.DISPLAY_MAX_COLUMN_WIDTH;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_MAX_TABLE_RESULT_ROWS;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;

/** Describes a result to be expected from a table program. */
public class ResultDescriptor {

    private final StatementResult tableResult;
    private final ReadableConfig config;

    public ResultDescriptor(StatementResult tableResult, ReadableConfig config) {
        this.tableResult = tableResult;
        this.config = config;
    }

    @SuppressWarnings("unchecked")
    public <T extends DynamicResult> T createResult() {
        ResultMode resultMode = config.get(EXECUTION_RESULT_MODE);
        boolean isStreaming = config.get(RUNTIME_MODE).equals(RuntimeExecutionMode.STREAMING);
        if (resultMode.equals(CHANGELOG) && !isStreaming) {
            throw new SqlExecutionException(
                    "Results of batch queries can only be served in table or tableau mode.");
        }

        switch (resultMode) {
            case CHANGELOG:
            case TABLEAU:
                return (T) new ChangelogCollectResult(tableResult);
            case TABLE:
                Integer maxRows = config.get(EXECUTION_MAX_TABLE_RESULT_ROWS);
                if (isStreaming) {
                    return (T) new MaterializedCollectStreamResult(tableResult, maxRows);
                } else {
                    return (T) new MaterializedCollectBatchResult(tableResult, maxRows);
                }
            default:
                throw new SqlExecutionException(
                        String.format(
                                "Unknown value '%s' for option '%s'.",
                                resultMode, EXECUTION_RESULT_MODE.key()));
        }
    }

    public ResolvedSchema getResultSchema() {
        return tableResult.getResultSchema();
    }

    public boolean isMaterialized() {
        return config.get(EXECUTION_RESULT_MODE).equals(TABLE);
    }

    public boolean isTableauMode() {
        return config.get(EXECUTION_RESULT_MODE).equals(ResultMode.TABLEAU);
    }

    public boolean isStreamingMode() {
        return config.get(RUNTIME_MODE).equals(RuntimeExecutionMode.STREAMING);
    }

    public int maxColumnWidth() {
        return config.get(DISPLAY_MAX_COLUMN_WIDTH);
    }

    public RowDataToStringConverter getRowDataStringConverter() {
        return tableResult.getRowDataToStringConverter();
    }
}
