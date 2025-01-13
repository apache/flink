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

package org.apache.flink.table.runtime.operators.window.async;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.MergeCallback;
import org.apache.flink.table.runtime.operators.window.Window;

import javax.annotation.Nullable;

/**
 * Callback to be used in when merging slices or windows for specifying which slices or windows
 * should be merged.
 *
 * <p>Different with {@link MergeCallback}, this callback maybe merge slices or windows to async
 * state.
 *
 * @param <W> The type {@link Window} for windows or the type {@link Long} for slices that this
 *     callback used to merge.
 * @param <R> The result type like {@link java.util.Collection} or {@link Iterable} to specify which
 *     slices or windows should be merged.
 */
public interface AsyncMergeCallback<W, R> {

    /**
     * Specifies that states of the given windows or slices should be merged into the result window
     * or slice.
     *
     * @param mergeResult The resulting merged window or slice, {@code null} if it represents a
     *     non-state namespace.
     * @param toBeMerged Windows or slices that should be merged into one window or slice.
     * @param resultNamespace The window or slice used as namespace to get the result from the
     *     merged accumulator.
     * @return f0 is the accumulators after merging, f1 is the result of the aggregation from the
     *     merged accumulators with this slice end as namespace
     */
    StateFuture<Tuple2<RowData, RowData>> asyncMerge(
            @Nullable W mergeResult, R toBeMerged, W resultNamespace) throws Exception;
}
