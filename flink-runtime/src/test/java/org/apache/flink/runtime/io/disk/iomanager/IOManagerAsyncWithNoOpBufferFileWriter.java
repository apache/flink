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

import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/**
 * An {@link IOManagerAsync} that creates {@link BufferFileWriter} instances which do nothing in their {@link BufferFileWriter#writeBlock(Object)} method.
 *
 * <p>Beware: the passed {@link Buffer} instances must be cleaned up manually!
 */
public class IOManagerAsyncWithNoOpBufferFileWriter extends IOManagerAsync {
	@Override
	public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID)
			throws IOException {
		return new NoOpAsynchronousBufferFileWriter(channelID, getWriteRequestQueue(channelID));
	}

	/**
	 * {@link BufferFileWriter} subclass with a no-op in {@link #writeBlock(Buffer)}.
	 */
	private static class NoOpAsynchronousBufferFileWriter extends AsynchronousBufferFileWriter {

		private NoOpAsynchronousBufferFileWriter(
				ID channelID,
				RequestQueue<WriteRequest> requestQueue) throws IOException {
			super(channelID, requestQueue);
		}

		@Override
		public void writeBlock(Buffer buffer) throws IOException {
			// do nothing
		}
	}
}
