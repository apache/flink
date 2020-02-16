/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;

import java.util.Optional;

/**
 * Always empty implementation of {@link BufferStorage}. It doesn't allow for adding any data.
 */
@Internal
public class EmptyBufferStorage implements BufferStorage {
	@Override
	public void add(BufferOrEvent boe) {
		throw new UnsupportedOperationException("Adding to EmptyBufferStorage is unsupported");
	}

	@Override
	public boolean isFull() {
		return false;
	}

	@Override
	public void rollOver() {
	}

	@Override
	public long getPendingBytes() {
		return 0;
	}

	@Override
	public long getRolledBytes() {
		return 0;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public Optional<BufferOrEvent> pollNext() {
		return Optional.empty();
	}

	@Override
	public long getMaxBufferedBytes() {
		return -1;
	}

	@Override
	public void close() {
	}
}
