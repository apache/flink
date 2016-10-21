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

import com.google.common.annotations.VisibleForTesting;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A HDFS subpartition, which is able to spill to hdfs.
 *
 * <p> Buffers are all write to HDFS.
 */
class DFSSubpartition extends ResultSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(DFSSubpartition.class);

	/** The writer used for write buffer to dfs file. */
	DFSFileWriter dfsWriter;

	/** Flag indicating whether the subpartition has been finished. */
	private boolean isFinished;

	/** Flag indicating whether the subpartition has been released. */
	private volatile boolean isReleased;

	DFSSubpartition(int index, ResultPartition parent, String filename) {
		super(index, parent);
		dfsWriter = new DFSFileWriter(filename);
	}

	@Override
	public boolean add(Buffer buffer) throws IOException {
		checkNotNull(buffer);

		if (isFinished || isReleased) {
			return false;
		}

		dfsWriter.write(buffer);

		return true;
	}

	@Override
	public void finish() throws IOException {
		if (add(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE))) {
			isFinished = true;
		}

		// close the hdfsWriter to commit the file
		if (dfsWriter != null) {
			dfsWriter.close();
		}
	}

	@Override
	public void release() throws IOException {
		if (isReleased) {
			return;
		}

		dfsWriter.close();
		isReleased = true;
	}

	@Override
	public int releaseMemory() throws IOException {
		dfsWriter.flush();
		return 0;
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public ResultSubpartitionView createReadView(BufferProvider bufferProvider) throws IOException {
		throw new UnsupportedOperationException("Not support createReadView on HDFSSubPartition");
	}

	@Override
	public String toString() {
		return String.format("DFSSubpartition," +
						"finished? %s, read view? %s, writer? %s]",
				isFinished, null,	dfsWriter != null);
	}

	@VisibleForTesting
	void setDFSWriter(DFSFileWriter writer) {
		this.dfsWriter = writer;
	}
}
