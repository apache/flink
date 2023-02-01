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
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;

/** Collects results and returns them as a changelog. */
public class ChangelogCollectResult extends CollectResultBase implements ChangelogResult {

    private final List<RowData> changeRecordBuffer;
    @VisibleForTesting protected static final int CHANGE_RECORD_BUFFER_SIZE = 5_000;

    public ChangelogCollectResult(StatementResult tableResult) {
        super(tableResult);
        // prepare for changelog
        changeRecordBuffer = new ArrayList<>();
        retrievalThread.start();
    }

    @Override
    public TypedResult<List<RowData>> retrieveChanges() {
        synchronized (resultLock) {
            // retrieval thread is alive return a record if available
            // but the program must not have failed
            if (isRetrieving() && executionException.get() == null) {
                if (changeRecordBuffer.isEmpty()) {
                    return TypedResult.empty();
                } else {
                    final List<RowData> change = new ArrayList<>(changeRecordBuffer);
                    changeRecordBuffer.clear();
                    resultLock.notify();
                    return TypedResult.payload(change);
                }
            }
            // retrieval thread is dead but there is still a record to be delivered
            else if (!isRetrieving() && !changeRecordBuffer.isEmpty()) {
                final List<RowData> change = new ArrayList<>(changeRecordBuffer);
                changeRecordBuffer.clear();
                return TypedResult.payload(change);
            }
            // no results can be returned anymore
            else {
                return handleMissingResult();
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected void processRecord(RowData row) {
        synchronized (resultLock) {
            // wait if the buffer is full
            if (changeRecordBuffer.size() >= CHANGE_RECORD_BUFFER_SIZE) {
                try {
                    resultLock.wait();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            changeRecordBuffer.add(row);
        }
    }
}
