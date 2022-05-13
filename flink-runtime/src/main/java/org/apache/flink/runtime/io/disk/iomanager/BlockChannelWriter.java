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

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A writer that writes data in blocks to a file channel. The writer receives the data blocks in the
 * form of {@link org.apache.flink.core.memory.MemorySegment}, which it writes entirely to the
 * channel, regardless of how space in the segment is used. The writing may be realized
 * synchronously, or asynchronously, depending on the implementation.
 */
public interface BlockChannelWriter<T> extends BlockChannelWriterWithCallback<T> {

    /**
     * Gets the next memory segment that has been written and is available again. This method blocks
     * until such a segment is available, or until an error occurs in the writer, or the writer is
     * closed.
     *
     * <p>NOTE: If this method is invoked without any segment ever returning (for example, because
     * the {@link #writeBlock} method has not been invoked accordingly), the method may block
     * forever.
     *
     * @return The next memory segment from the writers's return queue.
     * @throws IOException Thrown, if an I/O error occurs in the writer while waiting for the
     *     request to return.
     */
    T getNextReturnedBlock() throws IOException;

    /**
     * Gets the queue in which the memory segments are queued after the asynchronous write is
     * completed
     *
     * @return The queue with the written memory segments.
     */
    LinkedBlockingQueue<T> getReturnQueue();
}
