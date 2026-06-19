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

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Consumer;

/** The default fetch task that fetches the records into the element queue. */
@Internal
class FetchTask<E, SplitT extends SourceSplit> implements SplitFetcherTask {
    private final SplitReader<E, SplitT> splitReader;
    private final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue;
    private final Consumer<Collection<String>> splitFinishedCallback;
    private final int fetcherIndex;
    private volatile RecordsWithSplitIds<E> lastRecords;
    private volatile boolean wakeup;

    FetchTask(
            SplitReader<E, SplitT> splitReader,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Consumer<Collection<String>> splitFinishedCallback,
            int fetcherIndex) {
        this.splitReader = splitReader;
        this.elementsQueue = elementsQueue;
        this.splitFinishedCallback = splitFinishedCallback;
        this.lastRecords = null;
        this.fetcherIndex = fetcherIndex;
        this.wakeup = false;
    }

    @Override
    public boolean run() throws IOException {
        try {
            if (!isWakenUp() && lastRecords == null) {
                lastRecords = splitReader.fetch();
            }

            if (!isWakenUp()) {
                // The order matters here. We must first put the last records into the queue.
                // This ensures the handling of the fetched records is atomic to wakeup.
                if (elementsQueue.put(fetcherIndex, lastRecords)) {
                    if (!lastRecords.finishedSplits().isEmpty()) {
                        // The callback does not throw InterruptedException.
                        splitFinishedCallback.accept(lastRecords.finishedSplits());
                    }
                    lastRecords = null;
                }
            }
        } catch (InterruptedException e) {
            // this should only happen on shutdown
            throw new IOException("Source fetch execution was interrupted", e);
        } finally {
            // clean up the potential wakeup effect. It is possible that the fetcher is waken up
            // after the clean up. In that case, either the wakeup flag will be set or the
            // running thread will be interrupted. The next invocation of run() will see that and
            // just skip.
            if (isWakenUp()) {
                wakeup = false;
            }
        }
        // The return value of fetch task does not matter.
        return true;
    }

    @Override
    public void wakeUp() {
        // Set the wakeup flag first.
        wakeup = true;
        if (lastRecords == null) {
            // Two possible cases:
            // 1. The splitReader is reading or is about to read the records.
            // 2. The records has been enqueued and set to null.
            // In case 1, we just wakeup the split reader. In case 2, the next run might be skipped.
            // In any case, the records won't be enqueued in the ongoing run().
            splitReader.wakeUp();
        } else {
            // The task might be blocking on enqueuing the records, just interrupt.
            elementsQueue.wakeUpPuttingThread(fetcherIndex);
        }
    }

    private boolean isWakenUp() {
        return wakeup;
    }

    @Override
    public String toString() {
        return "FetchTask";
    }
}
