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
package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.netty.NettyMessage;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import java.io.IOException;
import java.nio.channels.FileChannel;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Partition data for wrapping the required fields for both pipelined and bounded partitions.
 */
public abstract class PartitionData {

	private final DataType nextDataType;
	protected final int backlog;
	protected final int sequenceNumber;

	PartitionData(int backlog, DataType nextDataType, int sequenceNumber) {
		this.backlog = backlog;
		this.nextDataType = checkNotNull(nextDataType);
		this.sequenceNumber = sequenceNumber;
	}

	public abstract boolean isBuffer();

	/**
	 * Builds the respective netty message {@link org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse}
	 * or {@link org.apache.flink.runtime.io.network.netty.NettyMessage.FileRegionResponse} to be transported in network stack.
	 */
	public abstract NettyMessage buildMessage(InputChannelID id) throws IOException;

	public DataType getNextDataType() {
		return nextDataType;
	}

	/**
	 * The pipelined partition or mmap-based bounded partition provide the buffer-level data to be consumed.
	 */
	static final class PartitionBuffer extends PartitionData {

		private final Buffer buffer;

		PartitionBuffer(Buffer buffer, int backlog, DataType nextDataType, int sequenceNumber) {
			super(backlog, nextDataType, sequenceNumber);
			this.buffer = checkNotNull(buffer);
		}

		@Override
		public NettyMessage buildMessage(InputChannelID id) {
			return new NettyMessage.BufferResponse(
				buffer,
				new NettyMessage.ResponseInfo(
					id,
					sequenceNumber,
					backlog,
					buffer.getDataType(),
					buffer.isCompressed(),
					buffer.readableBytes()));
		}

		@Override
		public boolean isBuffer() {
			return buffer.isBuffer();
		}
	}

	/**
	 * The file-based bounded partition provides FileRegion-level data to be consumed.
	 */
	static final class PartitionFileRegion extends PartitionData {

		private final FileChannel fileChannel;
		private final int dataSize;
		private final DataType dataType;
		private final boolean isCompressed;

		PartitionFileRegion(
				FileChannel fileChannel,
				int dataSize,
				DataType dataType,
				boolean isCompressed,
				DataType nextDataType,
				int backlog,
				int sequenceNumber) {

			super(backlog, checkNotNull(nextDataType), sequenceNumber);
			this.fileChannel = checkNotNull(fileChannel);
			this.dataSize = dataSize;
			this.dataType = checkNotNull(dataType);
			this.isCompressed = isCompressed;
		}

		@Override
		public NettyMessage buildMessage(InputChannelID id) throws IOException {
			return new NettyMessage.FileRegionResponse(
				fileChannel,
				new NettyMessage.ResponseInfo(
					id,
					sequenceNumber,
					backlog,
					dataType,
					isCompressed,
					dataSize));
		}

		@Override
		public boolean isBuffer() {
			return dataType == DataType.DATA_BUFFER;
		}
	}
}
