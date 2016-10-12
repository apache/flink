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

package org.apache.flink.streaming.api.functions.async.buffer;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;

/**
 * Abstract implementation for {@link StreamElementEntry}
 *
 * @param <OUT> Output type.
 */
public abstract class AbstractBufferEntry<OUT> implements StreamElementEntry<OUT> {
	private final StreamElement streamElement;

	protected AbstractBufferEntry(StreamElement element) {
		this.streamElement = Preconditions.checkNotNull(element, "Reference to StreamElement should not be null");
	}

	@Override
	public List<OUT> getResult() throws IOException {
		throw new UnsupportedOperationException("It is only available for StreamRecordEntry");
	}

	@Override
	public void markDone() {
		throw new UnsupportedOperationException("It is only available for StreamRecordEntry");
	}

	@Override
	public boolean isDone() {
		throw new UnsupportedOperationException("It must be overriden by the concrete entry");
	}

	@Override
	public boolean isStreamRecord() {
		return streamElement.isRecord();
	}

	@Override
	public boolean isWatermark() {
		return streamElement.isWatermark();
	}

	@Override
	public boolean isLatencyMarker() {
		return streamElement.isLatencyMarker();
	}

	@Override
	public StreamElement getStreamElement() {
		return streamElement;
	}

	@Override
	public String toString() {
		return "StreamElementEntry for @" + streamElement;
	}
}
