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

import org.apache.flink.table.temptable.TableService;
import org.apache.flink.table.temptable.util.BytesUtil;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * TableServiceServerHandler is responsible for the RPC request of TableService at server side.
 */
public class TableServiceServerHandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(TableServiceServerHandler.class);

	private TableService tableService;

	public TableService getTableService() {
		return tableService;
	}

	public void setTableService(TableService tableService) {
		this.tableService = tableService;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		ByteBuf buf = (ByteBuf) msg;
		byte[] request = new byte[buf.readableBytes()];
		buf.readBytes(request);
		byte call = request[0];
		switch (call) {
			case TableServiceMessage.GET_PARTITIONS:
				getPartitions(ctx, request);
				break;
			case TableServiceMessage.READ:
				read(ctx, request);
				break;
			case TableServiceMessage.WRITE:
				write(ctx, request);
				break;
			case TableServiceMessage.INITIALIZE_PARTITION:
				initializePartition(ctx, request);
				break;
			default:
				LOG.error("Unsupported call: " + call);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		LOG.error(cause.getMessage(), cause);
		ctx.close();
	}

	private void getPartitions(ChannelHandlerContext ctx, byte[] request) {
		int offset = 1;
		try {
			String tableName = new String(request, offset, request.length - offset, "UTF-8");
			List<Integer> result = tableService.getPartitions(tableName);
			sendGetPartitionsResponse(ctx, result);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			sendError(ctx, e.getMessage());
		}
	}

	private void read(ChannelHandlerContext ctx, byte[] request) {
		int offset = 1;
		try {
			int tableNameLength = BytesUtil.bytesToInt(request, offset);
			offset += Integer.BYTES;
			String tableName = new String(request, offset, tableNameLength, "UTF-8");
			offset += tableNameLength;
			int partitionId = BytesUtil.bytesToInt(request, offset);
			offset += Integer.BYTES;
			int readOffset = BytesUtil.bytesToInt(request, offset);
			offset += Integer.BYTES;
			int readCount = BytesUtil.bytesToInt(request, offset);
			offset += Integer.BYTES;
			byte[] result = tableService.read(tableName, partitionId, readOffset, readCount);
			sendReadResponse(ctx, result);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			LOG.error(e.getMessage(), e);
			sendError(ctx, e.getMessage());
		}

	}

	private void write(ChannelHandlerContext ctx, byte[] request) {
		int offset = 1;
		try {
			int tableNameLength = BytesUtil.bytesToInt(request, offset);
			offset += Integer.BYTES;
			String tableName = new String(request, offset, tableNameLength, "UTF-8");
			offset += tableNameLength;
			int partitionId = BytesUtil.bytesToInt(request, offset);
			offset += Integer.BYTES;
			byte[] content = new byte[request.length - offset];
			System.arraycopy(request, offset, content, 0, request.length - offset);
			LOG.info("tableName: " + tableName + ", partitionId: " + partitionId);
			int writeSize = tableService.write(tableName, partitionId, content);
			sendWriteResponse(ctx, writeSize);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			sendError(ctx, e.getMessage());
		}
	}

	private void initializePartition(ChannelHandlerContext ctx, byte[] request) {
		int offset = 1;
		try {
			int tableNameLength = BytesUtil.bytesToInt(request, offset);
			offset += Integer.BYTES;
			String tableName = new String(request, offset, tableNameLength, "UTF-8");
			offset += tableNameLength;
			int partitionId = BytesUtil.bytesToInt(request, offset);
			offset += Integer.BYTES;
			tableService.initializePartition(tableName, partitionId);
			sendInitializePartitionResponse(ctx);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			sendError(ctx, e.getMessage());
		}
	}

	private void sendError(ChannelHandlerContext ctx, String msg) {
		try {
			byte[] msgBytes = msg.getBytes("UTF-8");

			int totalLength = Integer.BYTES + Byte.BYTES + Integer.BYTES + msgBytes.length;
			byte[] totalLengthBytes = BytesUtil.intToBytes(totalLength);
			byte[] msgLengthBytes = BytesUtil.intToBytes(msgBytes.length);

			ByteBuf buffer = Unpooled.wrappedBuffer(
				totalLengthBytes,
				TableServiceMessage.FAILURE_BYTES,
				msgLengthBytes,
				msgBytes
			);
			ctx.writeAndFlush(buffer);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	private void sendGetPartitionsResponse(ChannelHandlerContext ctx, List<Integer> result) {

		int resultLength = result == null ? 0 : result.size();

		byte[] resultLengthBytes = BytesUtil.intToBytes(resultLength);
		byte[] resultBytes = BytesUtil.intsToBytes(result);

		int totalLength = Integer.BYTES + Byte.BYTES + Integer.BYTES + resultBytes.length;
		byte[] totalLengthBytes = BytesUtil.intToBytes(totalLength);

		ByteBuf buffer = Unpooled.wrappedBuffer(
			totalLengthBytes,
			TableServiceMessage.SUCCESS_BYTES,
			resultLengthBytes,
			resultBytes);

		ctx.writeAndFlush(buffer);
	}

	private void sendWriteResponse(ChannelHandlerContext ctx, int writeSize) {
		int totalLength = Integer.BYTES + Byte.BYTES + Integer.BYTES;
		byte[] writeSizeBytes = BytesUtil.intToBytes(writeSize);
		byte[] totalLengthBytes = BytesUtil.intToBytes(totalLength);
		ByteBuf buffer = Unpooled.wrappedBuffer(
			totalLengthBytes,
			TableServiceMessage.SUCCESS_BYTES,
			writeSizeBytes
		);
		ctx.writeAndFlush(buffer);
	}

	private void sendReadResponse(ChannelHandlerContext ctx, byte[] content) {
		int totalLength = Integer.BYTES + Byte.BYTES + content.length;
		byte[] totalLengthBytes = BytesUtil.intToBytes(totalLength);
		ByteBuf buffer = Unpooled.wrappedBuffer(
			totalLengthBytes,
			TableServiceMessage.SUCCESS_BYTES,
			content
		);
		ctx.writeAndFlush(buffer);
	}

	private void sendInitializePartitionResponse(ChannelHandlerContext ctx) {
		int totalLength = Integer.BYTES + Byte.BYTES;
		byte[] totalLengthBytes = BytesUtil.intToBytes(totalLength);
		ByteBuf buffer = Unpooled.wrappedBuffer(
			totalLengthBytes,
			TableServiceMessage.SUCCESS_BYTES
		);
		ctx.writeAndFlush(buffer);
	}
}
