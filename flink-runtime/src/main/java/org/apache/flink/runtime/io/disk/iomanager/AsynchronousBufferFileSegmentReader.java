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
import java.util.concurrent.atomic.AtomicBoolean;

public class AsynchronousBufferFileSegmentReader extends AsynchronousFileIOChannel<FileSegment, ReadRequest> implements BufferFileSegmentReader {

	private final AtomicBoolean hasReachedEndOfFile = new AtomicBoolean();

	protected AsynchronousBufferFileSegmentReader(ID channelID, RequestQueue<ReadRequest> requestQueue, RequestDoneCallback<FileSegment> callback) throws IOException {
		super(channelID, requestQueue, callback, false);
	}

	@Override
	public void read() throws IOException {
		addRequest(new FileSegmentReadRequest(this, hasReachedEndOfFile));
	}

	@Override
	public void seekTo(long position) throws IOException {
		requestQueue.add(new SeekRequest(this, position));
	}

	@Override
	public boolean hasReachedEndOfFile() {
		return hasReachedEndOfFile.get();
	}
}
