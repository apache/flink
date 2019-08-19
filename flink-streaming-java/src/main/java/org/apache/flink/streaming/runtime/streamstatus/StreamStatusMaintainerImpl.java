/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamstatus;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implements a stream status maintainer to emit {@link StreamStatus} via
 * {@link RecordWriterOutput} when the status has changed.
 */
@Internal
public class StreamStatusMaintainerImpl implements StreamStatusMaintainer {

	private final RecordWriterOutput<?>[] recordWriterOutputs;

	private StreamStatus currentStatus = StreamStatus.ACTIVE;

	public StreamStatusMaintainerImpl(RecordWriterOutput<?>[] recordWriterOutputs) {
		this.recordWriterOutputs = checkNotNull(recordWriterOutputs);
	}

	@Override
	public StreamStatus getStreamStatus() {
		return currentStatus;
	}

	@Override
	public void toggleStreamStatus(StreamStatus streamStatus) {
		if (streamStatus.equals(currentStatus)) {
			return;
		}

		currentStatus = streamStatus;
		// try and forward the stream status change to all outgoing connections
		for (RecordWriterOutput<?> output : recordWriterOutputs) {
			output.emitStreamStatus(streamStatus);
		}
	}
}
