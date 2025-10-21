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
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.VectorSearchTableSource;
import org.apache.flink.table.connector.source.search.AsyncVectorSearchFunctionProvider;
import org.apache.flink.table.connector.source.search.VectorSearchFunctionProvider;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.runtime.connector.source.VectorSearchRuntimeProviderContext;

import java.util.Collection;

/** Utils for {@code VECTOR_SEARCH}. */
public class VectorSearchUtil extends FunctionCallUtil {

    public static boolean isAsyncVectorSearch(
            TableSourceTable searchTable, Collection<Integer> searchColumns) {
        VectorSearchTableSource.VectorSearchRuntimeProvider provider =
                createVectorSearchRuntimeProvider(searchTable, searchColumns);
        if (provider instanceof AsyncVectorSearchFunctionProvider) {
            return true;
        }
        if (provider instanceof VectorSearchFunctionProvider) {
            return false;
        }
        throw new TableException(
                String.format(
                        "Can not find valid implementation for search function for table %s.",
                        searchTable.contextResolvedTable().getIdentifier().asSummaryString()));
    }

    public static VectorSearchTableSource.VectorSearchRuntimeProvider
            createVectorSearchRuntimeProvider(
                    TableSourceTable searchTable, Collection<Integer> searchColumns) {
        int[][] indices = searchColumns.stream().map(i -> new int[] {i}).toArray(int[][]::new);
        VectorSearchTableSource tableSource = (VectorSearchTableSource) searchTable.tableSource();
        VectorSearchRuntimeProviderContext providerContext =
                new VectorSearchRuntimeProviderContext(indices, new Configuration());
        return tableSource.getSearchRuntimeProvider(providerContext);
    }

    public static AsyncOptions getAsyncOptions(
            TableConfig config, ChangelogMode inputChangelogMode) {
        int asyncBufferCapacity =
                config.get(
                        ExecutionConfigOptions
                                .TABLE_EXEC_ASYNC_VECTOR_SEARCH_MAX_CONCURRENT_OPERATIONS);
        long asyncTimeout =
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_VECTOR_SEARCH_TIMEOUT)
                        .toMillis();
        AsyncDataStream.OutputMode asyncOutputMode =
                convert(
                        inputChangelogMode,
                        config.get(
                                ExecutionConfigOptions.TABLE_EXEC_ASYNC_VECTOR_SEARCH_OUTPUT_MODE));
        return new AsyncOptions(asyncBufferCapacity, asyncTimeout, false, asyncOutputMode);
    }
}
