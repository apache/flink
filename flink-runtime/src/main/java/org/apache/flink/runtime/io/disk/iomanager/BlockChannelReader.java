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
 * A reader that reads data in blocks from a file channel. The reader reads the blocks into a 
 * {@link org.apache.flink.core.memory.MemorySegment}. To support asynchronous implementations,
 * the read method does not immediately return the full memory segment, but rather adds it to
 * a blocking queue of finished read operations.
 */
public interface BlockChannelReader<T> extends FileIOChannel {

	/**
	 * Issues a read request, which will fill the given segment with the next block in the
	 * underlying file channel. Once the read request is fulfilled, the segment will be added to this reader's
	 * return queue.
	 *  
	 * @param segment The segment to read the block into.
	 * @throws IOException Thrown, when the reader encounters an I/O error.
	 */
	void readBlock(T segment) throws IOException;

	void seekToPosition(long position) throws IOException;
	
	/**
	 * Gets the next memory segment that has been filled with data by the reader. This method blocks until
	 * such a segment is available, or until an error occurs in the reader, or the reader is closed.
	 * <p>
	 * WARNING: If this method is invoked without any segment ever returning (for example, because the
	 * {@link #readBlock} method has not been invoked appropriately), the method may block
	 * forever.
	 * 
	 * @return The next memory segment from the reader's return queue.
	 * @throws IOException Thrown, if an I/O error occurs in the reader while waiting for the request to return.
	 */
	public T getNextReturnedBlock() throws IOException;
	
	/**
	 * Gets the queue in which the full memory segments are queued after the read is complete.
	 * 
	 * @return The queue with the full memory segments.
	 */
	LinkedBlockingQueue<T> getReturnQueue();
}
