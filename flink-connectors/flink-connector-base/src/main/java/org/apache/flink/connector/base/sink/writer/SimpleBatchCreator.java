/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.writer.strategy.RequestInfo;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A simple implementation of {@link BatchCreator} that forms a batch by taking up to {@code
 * requestInfo.getBatchSize()} entries from the head of the buffer, so long as the cumulative size
 * in bytes does not exceed the configured maximum.
 *
 * <p>This strategy stops gathering entries from the buffer as soon as adding the next entry would
 * exceed the {@code maxBatchSizeInBytes}, or once it has collected {@code
 * requestInfo.getBatchSize()} entries, whichever occurs first.
 *
 * @param <RequestEntryT> the type of request entries managed by this batch creator
 */
@Internal
public class SimpleBatchCreator<RequestEntryT extends Serializable>
        implements BatchCreator<RequestEntryT> {

    /** The maximum total byte size allowed for a single batch. */
    private final long maxBatchSizeInBytes;

    /**
     * Constructs a {@code SimpleBatchCreator} with the specified maximum batch size in bytes.
     *
     * @param maxBatchSizeInBytes the maximum cumulative size in bytes allowed for any single batch
     */
    public SimpleBatchCreator(long maxBatchSizeInBytes) {
        Preconditions.checkArgument(maxBatchSizeInBytes > 0);
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
    }

    /**
     * Creates the next batch of request entries based on the provided {@code requestInfo} and the
     * currently buffered entries.
     */
    @Override
    public Batch<RequestEntryT> createNextBatch(
            RequestInfo requestInfo, RequestBuffer<RequestEntryT> bufferedRequestEntries) {
        List<RequestEntryT> batch = new ArrayList<>(requestInfo.getBatchSize());
        long batchSizeBytes = 0L;

        for (int i = 0; i < requestInfo.getBatchSize() && !bufferedRequestEntries.isEmpty(); i++) {
            RequestEntryWrapper<RequestEntryT> peekedEntry = bufferedRequestEntries.peek();
            long requestEntrySize = peekedEntry.getSize();

            if (batchSizeBytes + requestEntrySize > maxBatchSizeInBytes) {
                break; // Stop if adding the next entry exceeds the byte limit
            }
            RequestEntryWrapper<RequestEntryT> elem = bufferedRequestEntries.poll();
            batch.add(elem.getRequestEntry());
            batchSizeBytes += requestEntrySize;
        }
        return new Batch<>(batch, batchSizeBytes);
    }
}
