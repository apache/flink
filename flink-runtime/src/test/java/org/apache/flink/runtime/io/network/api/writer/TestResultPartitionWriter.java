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

import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import java.io.IOException;

/**
 * A dummy implementation of {@link ResultPartitionWriter} used for tests.
 */
public class TestResultPartitionWriter implements ResultPartitionWriter {
	private final BufferProvider bufferProvider;

	public TestResultPartitionWriter(BufferProvider bufferProvider) {
		this.bufferProvider = bufferProvider;
	}

	@Override
	public BufferProvider getBufferProvider() {
		return bufferProvider;
	}

	@Override
	public BufferPool getBufferPool() {
		return null;
	}

	@Override
	public BufferPoolOwner getBufferPoolOwner() {
		return null;
	}

	@Override
	public ResultPartitionID getPartitionId() {
		return new ResultPartitionID();
	}

	@Override
	public ResultPartitionType getPartitionType() {
		return ResultPartitionType.PIPELINED;
	}

	@Override
	public ResultSubpartition[] getAllSubPartitions() {
		return new ResultSubpartition[1];
	}

	@Override
	public int getNumberOfSubpartitions() {
		return 1;
	}

	@Override
	public int getNumberOfQueuedBuffers() {
		return 1;
	}

	@Override
	public int getNumTargetKeyGroups() {
		return 1;
	}

	@Override
	public void registerBufferPool(BufferPool bufferPool) {
	}

	@Override
	public void destroyBufferPool() {
	}

	@Override
	public void addBufferConsumer(BufferConsumer bufferConsumer, int targetChannel) throws IOException {}

	@Override
	public void flushAll() {
	}

	@Override
	public void flush(int subpartitionIndex) {
	}

	@Override
	public ResultSubpartitionView createSubpartitionView(
			int index,
			BufferAvailabilityListener availabilityListener) {
		return null;
	}

	@Override
	public void release(Throwable cause) {
	}

	@Override
	public void finish() {
	}
}
