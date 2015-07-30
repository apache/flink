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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Either type for {@link Buffer} or {@link AbstractEvent} instances tagged with the channel index,
 * from which they were received.
 */
public class BufferOrEvent {

	private final Buffer buffer;

	private final AbstractEvent event;

	private int channelIndex;

	public BufferOrEvent(Buffer buffer, int channelIndex) {
		this.buffer = checkNotNull(buffer);
		this.event = null;
		this.channelIndex = channelIndex;
	}

	public BufferOrEvent(AbstractEvent event, int channelIndex) {
		this.buffer = null;
		this.event = checkNotNull(event);
		this.channelIndex = channelIndex;
	}

	public boolean isBuffer() {
		return buffer != null;
	}

	public boolean isEvent() {
		return event != null;
	}

	public Buffer getBuffer() {
		return buffer;
	}

	public AbstractEvent getEvent() {
		return event;
	}

	public int getChannelIndex() {
		return channelIndex;
	}

	public void setChannelIndex(int channelIndex) {
		checkArgument(channelIndex >= 0);
		this.channelIndex = channelIndex;
	}

	@Override
	public String toString() {
		return String.format("BufferOrEvent [%s, channelIndex = %d]",
				isBuffer() ? buffer : event, channelIndex);
	}
}
