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

package org.apache.flink.table.temptable.rpc;

import org.apache.flink.table.temptable.TableServiceException;
import org.apache.flink.table.temptable.util.BytesUtil;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * TableServiceClientHandler is responsible for the RPC request of TableService at client side.
 */
public class TableServiceClientHandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(TableServiceClientHandler.class);

	private ChannelHandlerContext context;

	private List<Integer> getPartitionsResult;

	private int writeResult;

	private byte[] readResult;

	private byte lastRequest;

	private String errorMsg;

	private boolean hasError;

	@Override
	public synchronized void channelActive(ChannelHandlerContext ctx) throws Exception {
		context = ctx;
		notify();
	}

	private synchronized void ensureConnectionReady() throws Exception {
		while (context == null) {
			wait();
		}
	}

	@Override
	public synchronized void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		ByteBuf buffer = (ByteBuf) msg;
		byte[] responseBytes = new byte[buffer.readableBytes()];
		buffer.readBytes(responseBytes);
		byte response = responseBytes[0];
		if (response == TableServiceMessage.SUCCESS) {
			switch (lastRequest) {
				case TableServiceMessage.GET_PARTITIONS:
					handleGetPartitionsResult(responseBytes);
					break;
				case TableServiceMessage.READ:
					handleReadResult(responseBytes);
					break;
				case TableServiceMessage.WRITE:
					handleWriteResult(responseBytes);
					break;
				case TableServiceMessage.INITIALIZE_PARTITION:
					handleInitializePartitionResult(responseBytes);
					break;
				default:
					LOG.error("Unsupported call: " + lastRequest);
			}
		} else {
			handleError(responseBytes);
		}
		notify();
	}

	private void handleGetPartitionsResult(byte[] response) {
		int offset = TableServiceMessage.RESPONSE_STATUS_LENGTH;
		int partitions = BytesUtil.bytesToInt(response, offset);
		offset += Integer.BYTES;
		if (partitions == 0) {
			getPartitionsResult = null;
		} else {
			getPartitionsResult = new ArrayList<>();
			for (int i = 0; i < partitions; i++) {
				int next = BytesUtil.bytesToInt(response, offset);
				offset += Integer.BYTES;
				getPartitionsResult.add(next);
			}
		}
		this.hasError = false;
	}

	private void handleReadResult(byte[] response) {
		int offset = TableServiceMessage.RESPONSE_STATUS_LENGTH;
		byte[] result = new byte[response.length - TableServiceMessage.RESPONSE_STATUS_LENGTH];
		System.arraycopy(response, offset, result, 0, response.length - 1);
		this.readResult = result;
		this.hasError = false;
	}

	private void handleWriteResult(byte[] response) {
		int offset = TableServiceMessage.RESPONSE_STATUS_LENGTH;
		int result = BytesUtil.bytesToInt(response, offset);
		this.writeResult = result;
		this.hasError = false;
	}

	private void handleInitializePartitionResult(byte[] response) {
		this.hasError = false;
	}

	public synchronized List<Integer> getPartitions(String tableName) throws Exception {
		ensureConnectionReady();
		byte[] tableNameInBytes = tableName.getBytes("UTF-8");

		int totalLength = Integer.BYTES + Byte.BYTES + tableNameInBytes.length;
		ByteBuf buffer = Unpooled.wrappedBuffer(
			BytesUtil.intToBytes(totalLength),
			TableServiceMessage.GET_PARTITIONS_BYTES,
			tableNameInBytes
		);
		context.writeAndFlush(buffer);
		lastRequest = TableServiceMessage.GET_PARTITIONS;
		wait();
		if (hasError) {
			hasError = false;
			throw new TableServiceException(new RuntimeException(errorMsg));
		}
		return getPartitionsResult == null ? Collections.emptyList() : getPartitionsResult;
	}

	public synchronized int write(String tableName, int partitionId, byte[] content) throws Exception {
		ensureConnectionReady();
		byte[] tableNameInBytes = tableName.getBytes("UTF-8");
		int contentLength = content == null ? 0 : content.length;

		int totalLength = Integer.BYTES + Byte.BYTES + Integer.BYTES + tableNameInBytes.length + Integer.BYTES + contentLength;

		ByteBuf buffer = Unpooled.wrappedBuffer(
			BytesUtil.intToBytes(totalLength),
			TableServiceMessage.WRITE_BYTES,
			BytesUtil.intToBytes(tableNameInBytes.length),
			tableNameInBytes,
			BytesUtil.intToBytes(partitionId),
			content
		);

		context.writeAndFlush(buffer);
		lastRequest = TableServiceMessage.WRITE;
		wait();
		if (hasError) {
			hasError = false;
			throw new RuntimeException(errorMsg);
		}
		return writeResult;
	}

	public synchronized byte[] read(String tableName, int partitionId, int offset, int readCount) throws Exception {
		ensureConnectionReady();
		byte[] tableNameInBytes = tableName.getBytes("UTF-8");
		int totalLength = Integer.BYTES + Byte.BYTES + Integer.BYTES + tableNameInBytes.length + Integer.BYTES * 3;

		ByteBuf buffer = Unpooled.wrappedBuffer(
			BytesUtil.intToBytes(totalLength),
			TableServiceMessage.READ_BYTES,
			BytesUtil.intToBytes(tableNameInBytes.length),
			tableNameInBytes,
			BytesUtil.intToBytes(partitionId),
			BytesUtil.intToBytes(offset),
			BytesUtil.intToBytes(readCount)
		);

		context.writeAndFlush(buffer);
		lastRequest = TableServiceMessage.READ;
		wait();
		if (hasError) {
			hasError = false;
			throw new TableServiceException(new RuntimeException(errorMsg));
		}
		return readResult;
	}

	public synchronized void initializePartition(String tableName, int partitionId) throws Exception {
		ensureConnectionReady();
		byte[] tableNameInBytes = tableName.getBytes("UTF-8");
		int totalLength = Integer.BYTES + Byte.BYTES + Integer.BYTES + tableNameInBytes.length + Integer.BYTES;

		ByteBuf buffer = Unpooled.wrappedBuffer(
			BytesUtil.intToBytes(totalLength),
			TableServiceMessage.INITIALIZE_PARTITION_BYTES,
			BytesUtil.intToBytes(tableNameInBytes.length),
			tableNameInBytes,
			BytesUtil.intToBytes(partitionId)
		);

		context.writeAndFlush(buffer);
		lastRequest = TableServiceMessage.INITIALIZE_PARTITION;
		wait();
		if (hasError) {
			hasError = false;
			throw new RuntimeException(errorMsg);
		}
	}

	private void handleError(byte[] response) {
		int offset = TableServiceMessage.RESPONSE_STATUS_LENGTH + Integer.BYTES;
		hasError = true;
		try {
			errorMsg = new String(response, offset, response.length - offset, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			LOG.error(e.getMessage(), e);
			errorMsg = "Unsupported msg encoding";
		}
	}
}
