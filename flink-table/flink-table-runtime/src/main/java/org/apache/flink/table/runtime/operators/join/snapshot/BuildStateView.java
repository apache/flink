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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/**
 * Keyed state holding the committed build side of a {@link LateralSnapshotJoinOperator}. All
 * methods operate on the build table of the operator's current key.
 *
 * <p>Implementations differ in how the build table is laid out: {@link MultiSetBuildStateView}
 * keeps a row → count multi-set, {@link UpsertBuildStateView} keys rows by their upsert key.
 */
interface BuildStateView {

    /** Declares the keyed state. Called once from the operator's {@code open()}. */
    void open(RuntimeContext ctx) throws Exception;

    /**
     * Applies a single committed build-side change. The change carries its original {@link
     * RowKind}; insertions/updates add or overwrite a row, retractions remove it.
     *
     * @return {@code false} if the change is a retraction of a row that is not in the table.
     */
    boolean apply(RowData change) throws Exception;

    /** Visits the committed build rows, each with the number of times it is present. */
    void forEachRow(RowVisitor visitor) throws Exception;

    /** Clears the committed build table (used by TTL eviction). */
    void clear() throws Exception;

    /** Callback of {@link #forEachRow(RowVisitor)}. */
    interface RowVisitor {
        void visit(RowData buildRow, long count) throws Exception;
    }
}
