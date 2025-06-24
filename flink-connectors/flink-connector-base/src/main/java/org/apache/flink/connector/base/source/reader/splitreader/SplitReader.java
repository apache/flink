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
import java.util.Collection;

/**
 * An interface used to read from splits. The implementation could either read from a single split
 * or from multiple splits.
 *
 * @param <E> the element type.
 * @param <SplitT> the split type.
 */
@PublicEvolving
public interface SplitReader<E, SplitT extends SourceSplit> extends AutoCloseable {

    /**
     * Fetch elements into the blocking queue for the given splits. The fetch call could be blocking
     * but it should get unblocked when {@link #wakeUp()} is invoked. In that case, the
     * implementation may either decide to return without throwing an exception, or it can just
     * throw an interrupted exception. In either case, this method should be reentrant, meaning that
     * the next fetch call should just resume from where the last fetch call was waken up or
     * interrupted. It is up to the implementer to either read all the records of the split or to
     * stop reading them at some point (for example when a given threshold is exceeded). In that
     * later case, when fetch is called again, the reading should restart at the record where it
     * left off based on the {@code SplitState}.
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
     * <p>For the consistency of internal state in SourceReaderBase, if a split is removed, it
     * should be put back into {@link RecordsWithSplitIds} as finished splits so that
     * SourceReaderBase could be able to clean up resources created for it.
     *
     * @param splitsChanges the split changes that the SplitReader needs to handle.
     */
    void handleSplitsChanges(SplitsChange<SplitT> splitsChanges);

    /** Wake up the split reader in case the fetcher thread is blocking in {@link #fetch()}. */
    void wakeUp();

    /**
     * Pauses or resumes reading of individual splits readers.
     *
     * <p>Note that no other methods can be called in parallel, so it's fine to non-atomically
     * update subscriptions. This method is simply providing connectors with more expressive APIs
     * the opportunity to update all subscriptions at once.
     *
     * <p>This is currently used to align the watermarks of splits, if watermark alignment is used
     * and the source reads from more than one split.
     *
     * <p>The default implementation throws an {@link UnsupportedOperationException} where the
     * default implementation will be removed in future releases. To be compatible with future
     * releases, it is recommended to implement this method and override the default implementation.
     *
     * @param splitsToPause the splits to pause
     * @param splitsToResume the splits to resume
     */
    default void pauseOrResumeSplits(
            Collection<SplitT> splitsToPause, Collection<SplitT> splitsToResume) {
        throw new UnsupportedOperationException(
                "This split reader does not support pausing or resuming splits which can lead to unaligned splits.\n"
                        + "Unaligned splits are splits where the output watermarks of the splits have diverged more than the allowed limit.\n"
                        + "It is highly discouraged to use unaligned source splits, as this leads to unpredictable\n"
                        + "watermark alignment if there is more than a single split per reader. It is recommended to implement pausing splits\n"
                        + "for this source. At your own risk, you can allow unaligned source splits by setting the\n"
                        + "configuration parameter `pipeline.watermark-alignment.allow-unaligned-source-splits' to true.\n"
                        + "Beware that this configuration parameter will be dropped in a future Flink release.");
    }
}
