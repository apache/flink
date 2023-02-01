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

package org.apache.flink.table.client.gateway.result;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;

/** Base class to collect results and returns them as table snapshots. */
public abstract class MaterializedCollectResultBase extends CollectResultBase
        implements MaterializedResult {

    /** Maximum initial capacity of the materialized table. */
    public static final int MATERIALIZED_TABLE_MAX_INITIAL_CAPACITY = 1_000_000;

    /** Maximum overcommitment of the materialized table. */
    public static final int MATERIALIZED_TABLE_MAX_OVERCOMMIT = 1_000_000;

    /** Factor for the initial capacity of the materialized table. */
    public static final double MATERIALIZED_TABLE_CAPACITY_FACTOR = 0.05;

    /** Factor for cleaning up deleted rows in the materialized table. */
    public static final double MATERIALIZED_TABLE_OVERCOMMIT_FACTOR = 0.01;

    /**
     * Maximum number of materialized rows to be stored. After the count is reached, oldest rows are
     * dropped.
     */
    protected final int maxRowCount;

    /** Threshold for cleaning up deleted rows in the materialized table. */
    protected final int overcommitThreshold;

    /**
     * Materialized table that is continuously updated by inserts and deletes. Deletes at the
     * beginning are lazily cleaned up when the threshold is reached.
     */
    protected final List<RowData> materializedTable;

    /** Counter for deleted rows to be deleted at the beginning of the materialized table. */
    protected int validRowPosition;

    /** Current snapshot of the materialized table. */
    private final List<RowData> snapshot;

    /** Page count of the snapshot (always >= 1). */
    private int pageCount;

    /** Page size of the snapshot (always >= 1). */
    private int pageSize;

    /** Indicator that this is the last snapshot possible (EOS afterwards). */
    private boolean isLastSnapshot;

    public MaterializedCollectResultBase(
            StatementResult tableResult, int maxRowCount, int overcommitThreshold) {
        super(tableResult);

        if (maxRowCount <= 0) {
            this.maxRowCount = Integer.MAX_VALUE;
        } else {
            this.maxRowCount = maxRowCount;
        }
        this.overcommitThreshold = overcommitThreshold;

        // prepare for materialization
        final int initialCapacity =
                computeMaterializedTableCapacity(maxRowCount); // avoid frequent resizing
        materializedTable = new ArrayList<>(initialCapacity);
        validRowPosition = 0;

        snapshot = new ArrayList<>();
        isLastSnapshot = false;

        pageCount = 0;
    }

    @Override
    public TypedResult<Integer> snapshot(int pageSize) {
        if (pageSize < 1) {
            throw new SqlExecutionException("Page size must be greater than 0.");
        }

        synchronized (resultLock) {
            // retrieval thread is dead and there are no results anymore
            // or program failed
            if ((!isRetrieving() && isLastSnapshot) || executionException.get() != null) {
                return handleMissingResult();
            }
            // this snapshot is the last result that can be delivered
            else if (!isRetrieving()) {
                isLastSnapshot = true;
            }

            this.pageSize = pageSize;
            snapshot.clear();
            for (int i = validRowPosition; i < materializedTable.size(); i++) {
                snapshot.add(materializedTable.get(i));
            }

            // at least one page
            pageCount = Math.max(1, (int) Math.ceil(((double) snapshot.size() / pageSize)));

            return TypedResult.payload(pageCount);
        }
    }

    @Override
    public List<RowData> retrievePage(int page) {
        synchronized (resultLock) {
            if (page <= 0 || page > pageCount) {
                throw new SqlExecutionException("Invalid page '" + page + "'.");
            }

            return snapshot.subList(
                    pageSize * (page - 1), Math.min(snapshot.size(), pageSize * page));
        }
    }

    protected static int computeMaterializedTableCapacity(int maxRowCount) {
        return Math.min(
                MATERIALIZED_TABLE_MAX_INITIAL_CAPACITY,
                Math.max(1, (int) (maxRowCount * MATERIALIZED_TABLE_CAPACITY_FACTOR)));
    }

    protected static int computeMaterializedTableOvercommit(int maxRowCount) {
        return Math.min(
                MATERIALIZED_TABLE_MAX_OVERCOMMIT,
                (int) (maxRowCount * MATERIALIZED_TABLE_OVERCOMMIT_FACTOR));
    }

    @VisibleForTesting
    protected List<RowData> getMaterializedTable() {
        return materializedTable;
    }
}
