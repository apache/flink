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
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import java.util.Map;

/** Collects results and returns them as table snapshots. */
public class MaterializedCollectStreamResult extends MaterializedCollectResultBase {

    /**
     * Caches the last row position for faster access. The position might not be exact (if rows with
     * smaller position are deleted) nor complete (for deletes of duplicates). However, the cache
     * narrows the search in the materialized table.
     */
    private final Map<RowData, Integer> rowPositionCache;

    @VisibleForTesting
    public MaterializedCollectStreamResult(
            StatementResult tableResult, int maxRowCount, int overcommitThreshold) {
        super(tableResult, maxRowCount, overcommitThreshold);

        final int initialCapacity =
                computeMaterializedTableCapacity(maxRowCount); // avoid frequent resizing
        rowPositionCache = CollectionUtil.newHashMapWithExpectedSize(initialCapacity);
        // start listener thread
        retrievalThread.start();
    }

    public MaterializedCollectStreamResult(StatementResult tableResult, int maxRowCount) {
        this(tableResult, maxRowCount, computeMaterializedTableOvercommit(maxRowCount));
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected void processRecord(RowData row) {
        synchronized (resultLock) {
            boolean isInsertOp =
                    row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER;
            // Always set the RowKind to INSERT, so that we can compare rows correctly (RowKind will
            // be ignored),
            row.setRowKind(RowKind.INSERT);

            // insert
            if (isInsertOp) {
                processInsert(row);
            }
            // delete
            else {
                processDelete(row);
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    private void processInsert(RowData row) {
        // limit the materialized table
        if (materializedTable.size() - validRowPosition >= maxRowCount) {
            cleanUp();
        }
        materializedTable.add(row);
        rowPositionCache.put(row, materializedTable.size() - 1);
    }

    private void processDelete(RowData row) {
        // delete the newest record first to minimize per-page changes
        final Integer cachedPos = rowPositionCache.get(row);
        final int startSearchPos;
        if (cachedPos != null) {
            startSearchPos = Math.min(cachedPos, materializedTable.size() - 1);
        } else {
            startSearchPos = materializedTable.size() - 1;
        }

        for (int i = startSearchPos; i >= validRowPosition; i--) {
            if (materializedTable.get(i).equals(row)) {
                materializedTable.remove(i);
                rowPositionCache.remove(row);
                break;
            }
        }
    }

    private void cleanUp() {
        // invalidate row
        final RowData deleteRow = materializedTable.get(validRowPosition);
        if (rowPositionCache.get(deleteRow) == validRowPosition) {
            // this row has no duplicates in the materialized table,
            // it can be removed from the cache
            rowPositionCache.remove(deleteRow);
        }
        materializedTable.set(validRowPosition, null);

        validRowPosition++;

        // perform clean up in batches
        if (validRowPosition >= overcommitThreshold) {
            materializedTable.subList(0, validRowPosition).clear();
            // adjust all cached indexes
            rowPositionCache.replaceAll((k, v) -> v - validRowPosition);
            validRowPosition = 0;
        }
    }
}
