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

package org.apache.flink.table.runtime.operators.window;

import javax.annotation.Nullable;

/**
 * Callback to be used in when merging slices or windows for specifying which slices or windows
 * should be merged.
 *
 * @param <W> The type {@link Window} for windows or the type {@link Long} for slices that this
 *     callback used to merge.
 * @param <R> The result type like {@link java.util.Collection} or {@link Iterable} to specify which
 *     slices or windows should be merged.
 */
public interface MergeCallback<W, R> {

    /**
     * Specifies that states of the given windows or slices should be merged into the result window
     * or slice.
     *
     * @param mergeResult The resulting merged window or slice, {@code null} if it represents a
     *     non-state namespace.
     * @param toBeMerged Windows or slices that should be merged into one window or slice.
     */
    void merge(@Nullable W mergeResult, R toBeMerged) throws Exception;
}
