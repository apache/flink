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
import org.apache.flink.connector.base.sink.writer.strategy.RequestInfo;

import java.io.Serializable;
import java.util.Deque;

/**
 * A pluggable interface for forming batches of request entries from a buffer. Implementations
 * control how many entries are grouped together and in what manner before sending them downstream.
 *
 * <p>The {@code AsyncSinkWriter} (or similar sink component) calls {@link
 * #createNextBatch(RequestInfo, RequestBuffer)} (RequestInfo, Deque)} when it decides to flush or
 * otherwise gather a new batch of elements. For instance, a batch creator might limit the batch by
 * the number of elements, total payload size, or any custom partition-based strategy.
 *
 * @param <RequestEntryT> the type of the request entries to be batched
 */
@PublicEvolving
public interface BatchCreator<RequestEntryT extends Serializable> {

    /**
     * Creates the next batch of request entries based on the provided {@link RequestInfo} and the
     * currently buffered entries.
     *
     * <p>This method is expected to:
     *
     * <ul>
     *   <li>Mutate the {@code bufferedRequestEntries} by polling/removing elements from it.
     *   <li>Return a batch containing the selected entries.
     * </ul>
     *
     * <p><strong>Thread-safety note:</strong> This method is called from {@code flush()}, which is
     * executed on the Flink main thread. Implementations should assume single-threaded access and
     * must not be shared across subtasks.
     *
     * <p><strong>Contract:</strong> Implementations must ensure that any entry removed from {@code
     * bufferedRequestEntries} is either added to the returned batch or properly handled (e.g.,
     * retried or logged), and not silently dropped.
     *
     * @param requestInfo information about the desired request properties or constraints (e.g., an
     *     allowed batch size or other relevant hints)
     * @param bufferedRequestEntries a collection ex: {@link Deque} of all currently buffered
     *     entries waiting to be grouped into batches
     * @return a {@link Batch} containing the new batch of entries along with metadata about the
     *     batch (e.g., total byte size, record count)
     */
    Batch<RequestEntryT> createNextBatch(
            RequestInfo requestInfo, RequestBuffer<RequestEntryT> bufferedRequestEntries);
}
