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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.FileSegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Either type for {@link Buffer} or {@link AbstractEvent} in-memory or backed
 * by a {@link FileSegment} instance.
 */
public class BufferOrEvent {

	public enum BufferOrEventType {
		BUFFER, EVENT
	}

	private final FileSegment fileSegment;

	private final Buffer buffer;

	private final AbstractEvent event;

	private final BufferOrEventType type;

	public BufferOrEvent(FileSegment fileSegment, BufferOrEventType type) {
		this(checkNotNull(fileSegment), null, null, checkNotNull(type));
	}

	public BufferOrEvent(Buffer buffer) {
		this(null, checkNotNull(buffer), null, BufferOrEventType.BUFFER);
	}

	public BufferOrEvent(AbstractEvent event) {
		this(null, null, checkNotNull(event), BufferOrEventType.EVENT);
	}

	private BufferOrEvent(FileSegment fileSegment, Buffer buffer, AbstractEvent event, BufferOrEventType type) {
		this.fileSegment = fileSegment;
		this.buffer = buffer;
		this.event = event;
		this.type = type;
	}

	public boolean isFileSegment() {
		return fileSegment != null;
	}

	public boolean isBuffer() {
		return type == BufferOrEventType.BUFFER;
	}

	public boolean isEvent() {
		return type == BufferOrEventType.EVENT;
	}

	public FileSegment getFileSegment() {
		return fileSegment;
	}

	public Buffer getBuffer() {
		return buffer;
	}

	public AbstractEvent getEvent() {
		return event;
	}

	@Override
	public String toString() {
		return "BufferOrEvent [" + (isFileSegment() ? (type + ", " + fileSegment) : (isBuffer() ? buffer : event)) + "]";
	}
}
