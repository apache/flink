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

package org.apache.flink.table.runtime.operators.join.snapshot;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/**
 * Keyed state holding the committed build side of a {@link LateralSnapshotJoinOperator}. All
 * methods operate on the build table of the operator's current key. Implementations declare their
 * keyed state when they are created, see {@link BuildStateViews#create}.
 */
@Internal
public interface BuildStateView {

    /**
     * Applies a single committed build-side change. The change carries its original {@link
     * RowKind}; insertions/updates add or overwrite a row, retractions remove it.
     *
     * @return {@code false} if the change is a retraction of a row that is not in the table.
     */
    boolean apply(RowData change) throws Exception;

    /**
     * Whether build changes must be applied in row-time order (gated by the build-side watermark)
     * instead of in arrival order. This is the case if a change can only be matched to its
     * counterpart by value, as opposed to by key.
     */
    boolean requiresRowtimeOrdering();

    /** Visits the committed build rows, each with the number of times it is present. */
    void forEachRow(RowVisitor visitor) throws Exception;

    /** Clears the committed build table (used by TTL eviction). */
    void clear();

    /** Callback of {@link #forEachRow(RowVisitor)}. */
    interface RowVisitor {
        void visit(RowData buildRow, long count) throws Exception;
    }
}
