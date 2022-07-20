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

package org.apache.flink.connector.base.source.reader.splitreader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import java.io.IOException;

/**
 * An interface used to read from splits. The implementation could either read from a single split
 * or from multiple splits.
 *
 * @param <E> the element type.
 * @param <SplitT> the split type.
 */
@PublicEvolving
public interface SplitReader<E, SplitT extends SourceSplit> {

    /**
     * Fetch elements into the blocking queue for the given splits. The fetch call could be blocking
     * but it should get unblocked when {@link #wakeUp()} is invoked. In that case, the
     * implementation may either decide to return without throwing an exception, or it can just
     * throw an interrupted exception. In either case, this method should be reentrant, meaning that
     * the next fetch call should just resume from where the last fetch call was waken up or
     * interrupted.
     *
     * @return the Ids of the finished splits.
     * @throws IOException when encountered IO errors, such as deserialization failures.
     */
    RecordsWithSplitIds<E> fetch() throws IOException;

    /**
     * Handle the split changes. This call should be non-blocking.
     *
     * <p>For the consistency of internal state in SourceReaderBase, if an invalid split is added to
     * the reader (for example splits without any records), it should be put back into {@link
     * RecordsWithSplitIds} as finished splits so that SourceReaderBase could be able to clean up
     * resources created for it.
     *
     * @param splitsChanges the split changes that the SplitReader needs to handle.
     */
    void handleSplitsChanges(SplitsChange<SplitT> splitsChanges);

    /** Wake up the split reader in case the fetcher thread is blocking in {@link #fetch()}. */
    void wakeUp();

    /**
     * Close the split reader.
     *
     * @throws Exception if closing the split reader failed.
     */
    void close() throws Exception;
}
