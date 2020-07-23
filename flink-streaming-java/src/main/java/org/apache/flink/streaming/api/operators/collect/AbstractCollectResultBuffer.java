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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * A buffer which encapsulates the logic of dealing with the response from the {@link CollectSinkFunction}.
 */
public abstract class AbstractCollectResultBuffer<T> {

	private static final String INIT_VERSION = "";

	private final TypeSerializer<T> serializer;
	private final LinkedList<T> buffer;

	// for detailed explanation of the following 2 variables, see Java doc of CollectSinkFunction
	// `version` is to check if the sink restarts
	private String version;
	// `offset` is the offset of the next result we want to fetch
	private long offset;

	// userVisibleHead <= user visible results offset < userVisibleTail
	private long userVisibleHead;
	private long userVisibleTail;

	public AbstractCollectResultBuffer(TypeSerializer<T> serializer) {
		this.serializer = serializer;
		this.buffer = new LinkedList<>();

		this.version = INIT_VERSION;
		this.offset = 0;

		this.userVisibleHead = 0;
		this.userVisibleTail = 0;
	}

	/**
	 * Get next user visible result, returns null if currently there is no more.
	 */
	public T next() {
		if (userVisibleHead == userVisibleTail) {
			return null;
		}
		T ret = buffer.removeFirst();
		userVisibleHead++;

		sanityCheck();
		return ret;
	}

	public long getOffset() {
		return offset;
	}

	public String getVersion() {
		return version;
	}

	public void dealWithResponse(CollectCoordinationResponse response, long responseOffset) throws IOException {
		String responseVersion = response.getVersion();
		long responseLastCheckpointedOffset = response.getLastCheckpointedOffset();

		if (!INIT_VERSION.equals(version) && !version.equals(responseVersion)) {
			// version not matched, sink has restarted
			sinkRestarted(responseLastCheckpointedOffset);
		}
		version = responseVersion;

		addResults(response, responseOffset);
		maintainVisibility(userVisibleTail, responseLastCheckpointedOffset);

		sanityCheck();
	}

	public void complete() {
		makeResultsVisible(offset);
	}

	protected abstract void sinkRestarted(long lastCheckpointedOffset);

	protected abstract void maintainVisibility(long currentVisiblePos, long lastCheckpointedOffset);

	protected void makeResultsVisible(long visiblePos) {
		userVisibleTail = visiblePos;
	}

	/**
	 * Revert the buffer back to the result whose offset is `checkpointedOffset`.
	 */
	protected void revert(long checkpointedOffset) {
		while (offset > checkpointedOffset) {
			buffer.removeLast();
			offset--;
		}
	}

	/**
	 * Clear the whole buffer and discard all results.
	 */
	protected void reset() {
		buffer.clear();
		userVisibleHead = 0;
		userVisibleTail = 0;
		offset = 0;
	}

	private void addResults(CollectCoordinationResponse response, long responseOffset) throws IOException {
		List<T> results = response.getResults(serializer);
		if (!results.isEmpty()) {
			// response contains some data, add them to buffer
			int addStart = (int) (offset - responseOffset);
			List<T> addedResults = results.subList(addStart, results.size());
			buffer.addAll(addedResults);
			offset += addedResults.size();
		}
	}

	private void sanityCheck() {
		Preconditions.checkState(
			userVisibleHead <= userVisibleTail,
			"userVisibleHead should not be larger than userVisibleTail. This is a bug.");
		Preconditions.checkState(
			userVisibleTail <= offset,
			"userVisibleTail should not be larger than offset. This is a bug.");
	}
}
