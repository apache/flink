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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Collection;

/**
 * A wrapper class of {@link TableFunction} for synchronous vector search.
 *
 * <p>The output type of this table function is fixed as {@link RowData}.
 */
@PublicEvolving
public abstract class VectorSearchFunction extends TableFunction<RowData> {

    /**
     * Synchronously search result based on input row to find topK matched rows.
     *
     * @param topK - The number of topK results to return.
     * @param queryData - A {@link RowData} that wraps input for vector search function.
     * @return A collection of predicted results.
     */
    public abstract Collection<RowData> vectorSearch(int topK, RowData queryData)
            throws IOException;

    /** Invoke {@link #vectorSearch} and handle exceptions. */
    public final void eval(Object... args) {
        int topK = (int) args[0];
        GenericRowData argsData = new GenericRowData(args.length - 1);
        for (int i = 1; i < args.length; ++i) {
            argsData.setField(i - 1, args[i]);
        }
        try {
            Collection<RowData> results = vectorSearch(topK, argsData);
            if (results == null) {
                return;
            }
            results.forEach(this::collect);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to execute search with input row %s.", argsData), e);
        }
    }
}
