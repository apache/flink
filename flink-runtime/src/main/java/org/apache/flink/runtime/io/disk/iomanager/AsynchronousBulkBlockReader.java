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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** */
public class AsynchronousBulkBlockReader
        extends AsynchronousFileIOChannel<MemorySegment, ReadRequest>
        implements BulkBlockChannelReader {

    private final ArrayList<MemorySegment> returnBuffers;

    protected AsynchronousBulkBlockReader(
            FileIOChannel.ID channelID,
            RequestQueue<ReadRequest> requestQueue,
            List<MemorySegment> sourceSegments,
            int numBlocks)
            throws IOException {
        this(
                channelID,
                requestQueue,
                sourceSegments,
                numBlocks,
                new ArrayList<MemorySegment>(numBlocks));
    }

    private AsynchronousBulkBlockReader(
            FileIOChannel.ID channelID,
            RequestQueue<ReadRequest> requestQueue,
            List<MemorySegment> sourceSegments,
            int numBlocks,
            ArrayList<MemorySegment> target)
            throws IOException {
        super(channelID, requestQueue, new CollectingCallback(target), false);
        this.returnBuffers = target;

        // sanity check
        if (sourceSegments.size() < numBlocks) {
            throw new IllegalArgumentException(
                    "The list of source memory segments must contain at least"
                            + " as many segments as the number of blocks to read.");
        }

        // send read requests for all blocks
        for (int i = 0; i < numBlocks; i++) {
            readBlock(sourceSegments.remove(sourceSegments.size() - 1));
        }
    }

    private void readBlock(MemorySegment segment) throws IOException {
        addRequest(new SegmentReadRequest(this, segment));
    }

    @Override
    public List<MemorySegment> getFullSegments() {
        synchronized (this.closeLock) {
            if (!this.isClosed() || this.requestsNotReturned.get() > 0) {
                throw new IllegalStateException(
                        "Full segments can only be obtained after the reader was properly closed.");
            }
        }

        return this.returnBuffers;
    }

    // --------------------------------------------------------------------------------------------

    private static final class CollectingCallback implements RequestDoneCallback<MemorySegment> {

        private final ArrayList<MemorySegment> list;

        public CollectingCallback(ArrayList<MemorySegment> list) {
            this.list = list;
        }

        @Override
        public void requestSuccessful(MemorySegment buffer) {
            list.add(buffer);
        }

        @Override
        public void requestFailed(MemorySegment buffer, IOException e) {
            list.add(buffer);
        }
    }
}
