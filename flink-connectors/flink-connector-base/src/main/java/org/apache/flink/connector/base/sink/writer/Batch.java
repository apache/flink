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

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.List;

/**
 * A container for the result of creating a batch of request entries, including:
 *
 * <ul>
 *   <li>The actual list of entries forming the batch
 *   <li>The total size in bytes of those entries
 *   <li>The total number of entries in the batch
 * </ul>
 *
 * <p>Instances of this class are typically created by a {@link BatchCreator} to summarize which
 * entries have been selected for sending downstream and to provide any relevant metrics for
 * tracking, such as the byte size or the record count.
 *
 * @param <RequestEntryT> the type of request entry in this batch
 */
@PublicEvolving
public class Batch<RequestEntryT extends Serializable> {

    /** The list of request entries in this batch. */
    private final List<RequestEntryT> batchEntries;

    /** The total size in bytes of the entire batch. */
    private final long sizeInBytes;

    /** The total number of entries in the batch. */
    private final int recordCount;

    /**
     * Creates a new {@code Batch} with the specified entries, total size, and record count.
     *
     * @param requestEntries the list of request entries that form the batch
     * @param sizeInBytes the total size in bytes of the entire batch
     */
    public Batch(List<RequestEntryT> requestEntries, long sizeInBytes) {
        this.batchEntries = requestEntries;
        this.sizeInBytes = sizeInBytes;
        this.recordCount = requestEntries.size();
    }

    /**
     * Returns the list of request entries in this batch.
     *
     * @return a list of request entries for the batch
     */
    public List<RequestEntryT> getBatchEntries() {
        return batchEntries;
    }

    /**
     * Returns the total size in bytes of the batch.
     *
     * @return the batch's cumulative byte size
     */
    public long getSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * Returns the total number of entries in the batch.
     *
     * @return the record count in the batch
     */
    public int getRecordCount() {
        return recordCount;
    }
}
