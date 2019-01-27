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

package org.apache.flink.streaming.runtime.streamstatus;

import java.util.BitSet;

/**
 * The sub maintainer of {@link StreamStatusMaintainer}.
 */
public class StreamStatusSubMaintainer implements StreamStatusMaintainer {

	private final StreamStatusMaintainer streamStatusMaintainer;

	private final BitSet subStatus;

	private final int subIndex;

	public StreamStatusSubMaintainer(
		StreamStatusMaintainer streamStatusMaintainer,
		BitSet subStatus,
		int subIndex) {

		this.streamStatusMaintainer = streamStatusMaintainer;
		this.subStatus = subStatus;
		this.subIndex = subIndex;

		this.subStatus.set(subIndex, true);
	}

	public void updateStreamStatus(StreamStatus streamStatus) {
		final StreamStatus previousStatus = getStreamStatus();
		if (previousStatus.equals(streamStatus)) {
			return;
		}
		subStatus.set(subIndex, streamStatus.isActive());
		if (streamStatus.isActive()) {
			// If there is an active sub maintainer, toggle active
			streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
		} else if (subStatus.isEmpty()) {
			// If all sub maintainer is idle, toggle idle
			streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
		}
	}

	@Override
	public StreamStatus getStreamStatus() {
		return subStatus.get(subIndex) ? StreamStatus.ACTIVE : StreamStatus.IDLE;
	}

	@Override
	public void toggleStreamStatus(StreamStatus streamStatus) {
		updateStreamStatus(streamStatus);
	}

	public void release() {
		this.subStatus.set(subIndex, false);
	}
}
