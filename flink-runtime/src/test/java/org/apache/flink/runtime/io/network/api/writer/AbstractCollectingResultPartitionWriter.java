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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.InternalResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.taskmanager.TaskActions;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayDeque;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
import static org.mockito.Mockito.mock;

/**
 * {@link ResultPartitionWriter} that collects output on the List.
 */
@ThreadSafe
public abstract class AbstractCollectingResultPartitionWriter extends InternalResultPartition {
	private final BufferProvider bufferProvider;
	private final ArrayDeque<BufferConsumer> bufferConsumers = new ArrayDeque<>();

	public AbstractCollectingResultPartitionWriter(BufferProvider bufferProvider) {
		super("TestTask",
			mock(TaskActions.class),
			new JobID(),
			new ResultPartitionID(),
			ResultPartitionType.PIPELINED,
			1,
			1,
			mock(ResultPartitionManager.class),
			mock(ResultPartitionConsumableNotifier.class),
			mock(IOManager.class),
			false);
		this.bufferProvider = checkNotNull(bufferProvider);
	}

	@Override
	public BufferProvider getBufferProvider() {
		return bufferProvider;
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
