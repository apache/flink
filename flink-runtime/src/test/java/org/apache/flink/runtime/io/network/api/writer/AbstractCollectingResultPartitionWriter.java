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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayDeque;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link ResultPartitionWriter} that collects output on the List.
 */
@ThreadSafe
public abstract class AbstractCollectingResultPartitionWriter implements ResultPartitionWriter {
	private final BufferProvider bufferProvider;
	private final ArrayDeque<BufferConsumer> bufferConsumers = new ArrayDeque<>();

	public AbstractCollectingResultPartitionWriter(BufferProvider bufferProvider) {
		this.bufferProvider = checkNotNull(bufferProvider);
	}

	@Override
	public BufferProvider getBufferProvider() {
		return bufferProvider;
	}

	@Override
	public ResultPartitionID getPartitionId() {
		return new ResultPartitionID();
	}

	@Override
	public int getNumberOfSubpartitions() {
		return 1;
	}

	@Override
	public int getNumTargetKeyGroups() {
		return 1;
	}

	@Override
	public synchronized void addBufferConsumer(BufferConsumer bufferConsumer, int targetChannel) throws IOException {
		checkState(targetChannel < getNumberOfSubpartitions());
		bufferConsumers.add(bufferConsumer);
		processBufferConsumers();
	}

	private void processBufferConsumers() throws IOException {
		while (!bufferConsumers.isEmpty()) {
			BufferConsumer bufferConsumer = bufferConsumers.peek();
			Buffer buffer = bufferConsumer.build();
			try {
				deserializeBuffer(buffer);
				if (!bufferConsumer.isFinished()) {
					break;
				}
				bufferConsumers.pop().close();
			}
			finally {
				buffer.recycleBuffer();
			}
		}
	}

	@Override
	public synchronized void flushAll() {
		try {
			processBufferConsumers();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void flush(int subpartitionIndex) {
		flushAll();
	}

	protected abstract void deserializeBuffer(Buffer buffer) throws IOException;
}
