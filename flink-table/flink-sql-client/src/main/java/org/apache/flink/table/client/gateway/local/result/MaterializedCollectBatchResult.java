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

package org.apache.flink.table.client.gateway.local.result;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/** Collects results and returns them as table snapshots. */
public class MaterializedCollectBatchResult extends MaterializedCollectResultBase {

    private static final Logger LOG = LoggerFactory.getLogger(MaterializedCollectResultBase.class);

    @VisibleForTesting
    public MaterializedCollectBatchResult(
            TableResultInternal tableResult, int maxRowCount, int overcommitThreshold) {
        super(tableResult, maxRowCount, overcommitThreshold);
        materializedTable = new ArrayList<>(maxRowCount);
        // start listener thread
        retrievalThread.start();
    }

    public MaterializedCollectBatchResult(TableResultInternal tableResult, int maxRowCount) {
        this(tableResult, maxRowCount, computeMaterializedTableOvercommit(maxRowCount));
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
            validRowPosition += snapshot.size();

            // at least one page
            pageCount = Math.max(1, (int) Math.ceil(((double) snapshot.size() / pageSize)));

            return TypedResult.payload(pageCount);
        }
    }

    @Override
    protected void processRecord(RowData row) {
        if (materializedTable.size() >= maxRowCount) {
            LOG.warn(
                    "materializedTable size exceed maxRowCount {}, discard row {}.",
                    maxRowCount,
                    row);
            return;
        }
        materializedTable.add(row);
    }
}
