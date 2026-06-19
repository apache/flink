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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * A wrapper class of {@link AsyncTableFunction} for asynchronous model inference.
 *
 * <p>The output type of this table function is fixed as {@link RowData}.
 */
@PublicEvolving
public abstract class AsyncPredictFunction extends AsyncTableFunction<RowData> {

    /**
     * Asynchronously predict result based on input row.
     *
     * @param inputRow - A {@link RowData} that wraps input for predict function.
     * @return A collection of all predicted results.
     */
    public abstract CompletableFuture<Collection<RowData>> asyncPredict(RowData inputRow);

    /** Invokes {@link #asyncPredict} and chains futures. */
    public void eval(CompletableFuture<Collection<RowData>> future, Object... args) {
        GenericRowData argsData = GenericRowData.of(args);
        asyncPredict(argsData)
                .whenComplete(
                        (result, exception) -> {
                            if (exception != null) {
                                future.completeExceptionally(
                                        new TableException(
                                                String.format(
                                                        "Failed to execute asynchronously prediction with input row %s.",
                                                        argsData),
                                                exception));
                                return;
                            }
                            future.complete(result);
                        });
    }
}
