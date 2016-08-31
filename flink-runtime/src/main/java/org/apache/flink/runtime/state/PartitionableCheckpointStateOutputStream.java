/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PartitionableCheckpointStateOutputStream extends FSDataOutputStream {

	private final Map<String, long[]> stateNameToPartitionOffsets;
	private final CheckpointStreamFactory.CheckpointStateOutputStream delegate;

	public PartitionableCheckpointStateOutputStream(CheckpointStreamFactory.CheckpointStateOutputStream delegate) {
		this.delegate = Preconditions.checkNotNull(delegate);
		this.stateNameToPartitionOffsets = new HashMap<>();
	}

	@Override
	public long getPos() throws IOException {
		return delegate.getPos();
	}

	@Override
	public void flush() throws IOException {
		delegate.flush();
	}

	@Override
	public void sync() throws IOException {
		delegate.sync();
	}

	@Override
	public void write(int b) throws IOException {
		delegate.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		delegate.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		delegate.write(b, off, len);
	}

	@Override
	public void close() throws IOException {
		delegate.close();
	}

	public OperatorStateHandle closeAndGetHandle() throws IOException {
		StreamStateHandle streamStateHandle = delegate.closeAndGetHandle();
		return new OperatorStateHandle(streamStateHandle, stateNameToPartitionOffsets);
	}

	public void startNewPartition(String stateName) throws IOException {
		long[] offs = stateNameToPartitionOffsets.get(stateName);
		if (offs == null) {
			offs = new long[1];
		} else {
			//TODO maybe we can use some primitive array list here instead of an array to avoid resize on each call.
			offs = Arrays.copyOf(offs, offs.length + 1);
		}

		offs[offs.length - 1] = getPos();
		stateNameToPartitionOffsets.put(stateName, offs);
	}

	public static PartitionableCheckpointStateOutputStream wrap(
			CheckpointStreamFactory.CheckpointStateOutputStream stream) {
		return new PartitionableCheckpointStateOutputStream(stream);
	}
}