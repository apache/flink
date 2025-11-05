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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;

import java.util.Map;

import static org.apache.flink.table.api.config.MLPredictRuntimeConfigOptions.ASYNC_MAX_CONCURRENT_OPERATIONS;
import static org.apache.flink.table.api.config.MLPredictRuntimeConfigOptions.ASYNC_OUTPUT_MODE;
import static org.apache.flink.table.api.config.MLPredictRuntimeConfigOptions.ASYNC_TIMEOUT;

/** Utils for {@code ML_PREDICT}. */
public class MLPredictUtil extends FunctionCallUtil {

    public static AsyncOptions getMergedMLPredictAsyncOptions(
            Map<String, String> runtimeConfig, TableConfig config) {
        Configuration queryConf = Configuration.fromMap(runtimeConfig);
        ExecutionConfigOptions.AsyncOutputMode asyncOutputMode =
                coalesce(
                        queryConf.get(ASYNC_OUTPUT_MODE),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_ML_PREDICT_OUTPUT_MODE));

        return new AsyncOptions(
                coalesce(
                        queryConf.get(ASYNC_MAX_CONCURRENT_OPERATIONS),
                        config.get(
                                ExecutionConfigOptions
                                        .TABLE_EXEC_ASYNC_ML_PREDICT_MAX_CONCURRENT_OPERATIONS)),
                coalesce(
                                queryConf.get(ASYNC_TIMEOUT),
                                config.get(
                                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_ML_PREDICT_TIMEOUT))
                        .toMillis(),
                false,
                convert(ChangelogMode.insertOnly(), asyncOutputMode));
    }
}
