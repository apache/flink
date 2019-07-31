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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;

import java.io.IOException;
import java.util.Optional;

/**
 * Implementation of {@link BufferStorage} that links two {@link BufferStorage} together.
 * Each of the linked {@link BufferStorage} will store buffers independently, but they will be
 * linked together for {@link #rollOver()} - if one is rolled over, other will do that as well.
 *
 * <p>Note that only {@code mainStorage} is closed when {@link LinkedBufferStorage} instance is closed.
 */
public class LinkedBufferStorage implements BufferStorage {

	private final BufferStorage mainStorage;

	private final BufferStorage linkedStorage;

	private long maxBufferedBytes;

	public LinkedBufferStorage(BufferStorage mainStorage, BufferStorage linkedStorage, long maxBufferedBytes) {
		this.mainStorage = mainStorage;
		this.linkedStorage = linkedStorage;
		this.maxBufferedBytes = maxBufferedBytes;
	}

	@Override
	public void add(BufferOrEvent boe) throws IOException {
		mainStorage.add(boe);
	}

	@Override
	public boolean isFull() {
		return maxBufferedBytes > 0 && (getRolledBytes() + getPendingBytes()) > maxBufferedBytes;
	}

	@Override
	public void rollOver() throws IOException {
		mainStorage.rollOver();
		linkedStorage.rollOver();
	}

	@Override
	public long getPendingBytes() {
		return mainStorage.getPendingBytes() + linkedStorage.getPendingBytes();
	}

	@Override
	public long getRolledBytes() {
		return mainStorage.getRolledBytes() + linkedStorage.getRolledBytes();
	}

	@Override
	public boolean isEmpty() {
		return mainStorage.isEmpty();
	}

	@Override
	public Optional<BufferOrEvent> pollNext() throws IOException {
		return mainStorage.pollNext();
	}

	@Override
	public long getMaxBufferedBytes() {
		return maxBufferedBytes;
	}

	@Override
	public void close() throws IOException {
		mainStorage.close();
	}
}
