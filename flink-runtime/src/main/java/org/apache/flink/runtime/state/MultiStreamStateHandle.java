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

import org.apache.flink.core.fs.AbstractMultiFSDataInputStream;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Wrapper class that takes multiple {@link StreamStateHandle} and makes them look like a single one. This is done by
 * providing a contiguous view on all the streams of the inner handles through a wrapper stream and by summing up all
 * all the meta data.
 */
public class MultiStreamStateHandle implements StreamStateHandle {

	private static final long serialVersionUID = -4588701089489569707L;
	private final List<StreamStateHandle> stateHandles;
	private final long stateSize;

	public MultiStreamStateHandle(List<StreamStateHandle> stateHandles) {
		this.stateHandles = Preconditions.checkNotNull(stateHandles);
		long calculateSize = 0L;
		for(StreamStateHandle stateHandle : stateHandles) {
			calculateSize += stateHandle.getStateSize();
		}
		this.stateSize = calculateSize;
	}

	@Override
	public FSDataInputStream openInputStream() throws IOException {
		return new MultiFSDataInputStream(stateHandles);
	}

	@Override
	public void discardState() throws Exception {
		StateUtil.bestEffortDiscardAllStateObjects(stateHandles);
	}

	@Override
	public long getStateSize() {
		return stateSize;
	}

	@Override
	public String toString() {
		return "MultiStreamStateHandle{" +
			"stateHandles=" + stateHandles +
			", stateSize=" + stateSize +
			'}';
	}

	static final class MultiFSDataInputStream extends AbstractMultiFSDataInputStream {

		private final TreeMap<Long, StreamStateHandle> stateHandleMap;

		public MultiFSDataInputStream(List<StreamStateHandle> stateHandles) throws IOException {
			this.stateHandleMap = new TreeMap<>();
			this.totalPos = 0L;
			long calculateSize = 0L;
			for (StreamStateHandle stateHandle : stateHandles) {
				stateHandleMap.put(calculateSize, stateHandle);
				calculateSize += stateHandle.getStateSize();
			}
			this.totalAvailable = calculateSize;

			if (totalAvailable > 0L) {
				StreamStateHandle first = stateHandleMap.firstEntry().getValue();
				delegate = first.openInputStream();
			}
		}

		@Override
		protected FSDataInputStream getSeekedStreamForOffset(long globalStreamOffset) throws IOException {
			Map.Entry<Long, StreamStateHandle> handleEntry = stateHandleMap.floorEntry(globalStreamOffset);
			if (handleEntry != null) {
				FSDataInputStream stream = handleEntry.getValue().openInputStream();
				stream.seek(globalStreamOffset - handleEntry.getKey());
				return stream;
			}
			return null;
		}
	}
}
