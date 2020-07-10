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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.local.result.ChangelogCollectResult;
import org.apache.flink.table.client.gateway.local.result.DynamicResult;
import org.apache.flink.table.client.gateway.local.result.MaterializedCollectBatchResult;
import org.apache.flink.table.client.gateway.local.result.MaterializedCollectStreamResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Maintains dynamic results. */
public class ResultStore {

    private final Map<String, DynamicResult> results;

    public ResultStore() {
        results = new HashMap<>();
    }

    /**
     * Creates a result. Might start threads or opens sockets so every created result must be
     * closed.
     */
    public DynamicResult createResult(Environment env, TableResult tableResult) {
        if (env.getExecution().inStreamingMode()) {
            if (env.getExecution().isChangelogMode() || env.getExecution().isTableauMode()) {
                return new ChangelogCollectResult(tableResult);
            } else {
                return new MaterializedCollectStreamResult(
                        tableResult, env.getExecution().getMaxTableResultRows());
            }
        } else {
            // Batch Execution
            if (env.getExecution().isTableMode()) {
                return new MaterializedCollectBatchResult(
                        tableResult, env.getExecution().getMaxTableResultRows());
            } else if (env.getExecution().isTableauMode()) {
                return new ChangelogCollectResult(tableResult);
            } else {
                throw new SqlExecutionException(
                        "Results of batch queries can only be served in table or tableau mode.");
            }
        }
    }

    public void storeResult(String resultId, DynamicResult result) {
        results.put(resultId, result);
    }

    public DynamicResult getResult(String resultId) {
        return results.get(resultId);
    }

    public void removeResult(String resultId) {
        results.remove(resultId);
    }

    public List<String> getResults() {
        return new ArrayList<>(results.keySet());
    }
}
