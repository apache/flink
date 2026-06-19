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
 * A wrapper class of {@link AsyncTableFunction} for asynchronously lookup rows matching the lookup
 * keys from external system.
 *
 * <p>The output type of this table function is fixed as {@link RowData}.
 */
@PublicEvolving
public abstract class AsyncLookupFunction extends AsyncTableFunction<RowData> {

    /**
     * Asynchronously lookup rows matching the lookup keys.
     *
     * <p>Please note that the returning collection of RowData shouldn't be reused across
     * invocations.
     *
     * @param keyRow - A {@link RowData} that wraps lookup keys.
     * @return A collection of all matching rows in the lookup table.
     */
    public abstract CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow);

    /** Invokes {@link #asyncLookup} and chains futures. */
    public final void eval(CompletableFuture<Collection<RowData>> future, Object... keys) {
        GenericRowData keyRow = GenericRowData.of(keys);
        asyncLookup(keyRow)
                .whenComplete(
                        (result, exception) -> {
                            if (exception != null) {
                                future.completeExceptionally(
                                        new TableException(
                                                String.format(
                                                        "Failed to asynchronously lookup entries with key '%s'",
                                                        keyRow),
                                                exception));
                                return;
                            }
                            future.complete(result);
                        });
    }
}
