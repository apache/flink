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

package org.apache.flink.runtime.io.network.partition;

import com.google.common.base.Optional;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;

import java.io.IOException;

public abstract class ResultSubpartition {

	protected final int index;

	protected final ResultPartition parent;

	// - Statistics ----------------------------------------------------------

	/** The total number of buffers (both data and event buffers) */
	private int totalNumberOfBuffers;

	/** The total number of bytes (both data and event buffers) */
	private long totalNumberOfBytes;

	public ResultSubpartition(int index, ResultPartition parent) {
		this.index = index;
		this.parent = parent;
	}

	protected void updateStatistics(Buffer buffer) {
		totalNumberOfBuffers++;
		totalNumberOfBytes += buffer.getSize();
	}

	protected void notifyConsumed() {
		parent.onConsumedSubpartition(index);
	}

	abstract void add(Buffer buffer) throws IOException;

	abstract void finish() throws IOException;

	abstract void release() throws IOException;

	abstract ResultSubpartitionView getReadView(Optional<BufferProvider> bufferProvider) throws IOException;

	abstract boolean isFinished();

	abstract int releaseMemory() throws IOException;

}
