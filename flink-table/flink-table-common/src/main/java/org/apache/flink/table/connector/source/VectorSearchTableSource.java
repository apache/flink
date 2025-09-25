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

package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.search.AsyncVectorSearchFunctionProvider;
import org.apache.flink.table.connector.source.search.VectorSearchFunctionProvider;
import org.apache.flink.types.RowKind;

import java.io.Serializable;

/**
 * A {@link DynamicTableSource} that search rows of an external storage system by one or more
 * vectors during runtime.
 *
 * <p>Compared to {@link ScanTableSource}, the source does not have to read the entire table and can
 * lazily fetch individual values from a (possibly continuously changing) external table when
 * necessary.
 *
 * <p>Note: Compared to {@link ScanTableSource}, a {@link VectorSearchTableSource} does only support
 * emitting insert-only changes currently (see also {@link RowKind}). Further abilities are not
 * supported.
 *
 * <p>In the last step, the planner will call {@link #getSearchRuntimeProvider(VectorSearchContext)}
 * for obtaining a provider of runtime implementation. The search fields that are required to
 * perform a search are derived from a query by the planner and will be provided in the given {@link
 * VectorSearchTableSource.VectorSearchContext#getSearchColumns()}. The values for those key fields
 * are passed during runtime.
 */
@PublicEvolving
public interface VectorSearchTableSource extends DynamicTableSource {

    /**
     * Returns a provider of runtime implementation for reading the data.
     *
     * <p>There exist different interfaces for runtime implementation which is why {@link
     * VectorSearchRuntimeProvider} serves as the base interface.
     *
     * <p>Independent of the provider interface, a source implementation can work on either
     * arbitrary objects or internal data structures (see {@link org.apache.flink.table.data} for
     * more information).
     *
     * <p>The given {@link VectorSearchContext} offers utilities by the planner for creating runtime
     * implementation with minimal dependencies to internal data structures.
     *
     * @see VectorSearchFunctionProvider
     * @see AsyncVectorSearchFunctionProvider
     */
    VectorSearchRuntimeProvider getSearchRuntimeProvider(VectorSearchContext context);

    // --------------------------------------------------------------------------------------------
    // Helper interfaces
    // --------------------------------------------------------------------------------------------

    /**
     * Context for creating runtime implementation via a {@link VectorSearchRuntimeProvider}.
     *
     * <p>It offers utilities by the planner for creating runtime implementation with minimal
     * dependencies to internal data structures.
     *
     * <p>Methods should be called in {@link #getSearchRuntimeProvider(VectorSearchContext)}.
     * Returned instances that are {@link Serializable} can be directly passed into the runtime
     * implementation class.
     */
    @PublicEvolving
    interface VectorSearchContext extends DynamicTableSource.Context {

        /**
         * Returns an array of key index paths that should be used during the search. The indices
         * are 0-based and support composite keys within (possibly nested) structures.
         *
         * <p>For example, given a table with data type {@code ROW < i INT, s STRING, r ROW < i2
         * INT, s2 STRING > >}, this method would return {@code [[0], [2, 1]]} when {@code i} and
         * {@code s2} are used for performing a lookup.
         *
         * @return array of key index paths
         */
        int[][] getSearchColumns();

        /**
         * Runtime config provided to provider. The config can be used by planner or vector search
         * provider at runtime. For example, async options can be used by planner to choose async
         * inference. Other config such as http timeout or retry can be used to configure search
         * functions.
         */
        ReadableConfig runtimeConfig();
    }

    /**
     * Provides actual runtime implementation for reading the data.
     *
     * <p>There exist different interfaces for runtime implementation which is why {@link
     * VectorSearchRuntimeProvider} serves as the base interface.
     *
     * @see VectorSearchFunctionProvider
     * @see AsyncVectorSearchFunctionProvider
     */
    @PublicEvolving
    interface VectorSearchRuntimeProvider {}
}
