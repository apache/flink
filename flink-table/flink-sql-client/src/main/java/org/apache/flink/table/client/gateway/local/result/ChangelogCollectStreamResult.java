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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Collects results and returns them as a changelog.
 *
 * @param <C> cluster id to which this result belongs to
 */
public class ChangelogCollectStreamResult<C> extends CollectStreamResult<C>
        implements ChangelogResult<C> {

    private List<Tuple2<Boolean, Row>> changeRecordBuffer;
    private static final int CHANGE_RECORD_BUFFER_SIZE = 5_000;

    public ChangelogCollectStreamResult(
            TableSchema tableSchema,
            ExecutionConfig config,
            InetAddress gatewayAddress,
            int gatewayPort) {
        super(tableSchema, config, gatewayAddress, gatewayPort);

        // prepare for changelog
        changeRecordBuffer = new ArrayList<>();
    }

    @Override
    public boolean isMaterialized() {
        return false;
    }

    @Override
    public TypedResult<List<Tuple2<Boolean, Row>>> retrieveChanges() {
        synchronized (resultLock) {
            // retrieval thread is alive return a record if available
            // but the program must not have failed
            if (isRetrieving() && executionException.get() == null) {
                if (changeRecordBuffer.isEmpty()) {
                    return TypedResult.empty();
                } else {
                    final List<Tuple2<Boolean, Row>> change = new ArrayList<>(changeRecordBuffer);
                    changeRecordBuffer.clear();
                    resultLock.notify();
                    return TypedResult.payload(change);
                }
            }
            // retrieval thread is dead but there is still a record to be delivered
            else if (!isRetrieving() && !changeRecordBuffer.isEmpty()) {
                final List<Tuple2<Boolean, Row>> change = new ArrayList<>(changeRecordBuffer);
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
    protected void processRecord(Tuple2<Boolean, Row> change) {
        synchronized (resultLock) {
            // wait if the buffer is full
            if (changeRecordBuffer.size() >= CHANGE_RECORD_BUFFER_SIZE) {
                try {
                    resultLock.wait();
                } catch (InterruptedException e) {
                    // ignore
                }
            } else {
                changeRecordBuffer.add(change);
            }
        }
    }
}
