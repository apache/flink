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

/** Collects results and returns them as table snapshots. */
public class MaterializedCollectBatchResult extends MaterializedCollectResultBase {

    @VisibleForTesting
    public MaterializedCollectBatchResult(
            StatementResult tableResult, int maxRowCount, int overcommitThreshold) {
        super(tableResult, maxRowCount, overcommitThreshold);
        // start listener thread
        retrievalThread.start();
    }

    public MaterializedCollectBatchResult(StatementResult tableResult, int maxRowCount) {
        this(tableResult, maxRowCount, computeMaterializedTableOvercommit(maxRowCount));
    }

    @Override
    protected void processRecord(RowData row) {
        // limit the materialized table
        if (materializedTable.size() - validRowPosition >= maxRowCount) {
            cleanUp();
        }
        materializedTable.add(row);
    }

    private void cleanUp() {
        materializedTable.set(validRowPosition, null);
        validRowPosition++;

        // perform clean up in batches
        if (validRowPosition >= overcommitThreshold) {
            materializedTable.subList(0, validRowPosition).clear();
            validRowPosition = 0;
        }
    }
}
