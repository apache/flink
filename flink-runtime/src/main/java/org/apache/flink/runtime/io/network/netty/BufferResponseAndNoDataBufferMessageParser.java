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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/**
 * The parser for both {@link NettyMessage.BufferResponse} and messages with no data buffer part.
 */
public class BufferResponseAndNoDataBufferMessageParser extends NoDataBufferMessageParser {
	/** The allocator for the flink buffer. */
	private final NetworkBufferAllocator networkBufferAllocator;

	BufferResponseAndNoDataBufferMessageParser(NetworkBufferAllocator networkBufferAllocator) {
		this.networkBufferAllocator = networkBufferAllocator;
	}

	@Override
	public int getMessageHeaderLength(int lengthWithoutFrameHeader, int msgId) {
		if (msgId == NettyMessage.BufferResponse.ID) {
			return NettyMessage.BufferResponse.MESSAGE_HEADER_LENGTH;
		} else {
			return super.getMessageHeaderLength(lengthWithoutFrameHeader, msgId);
		}
	}

	@Override
	public MessageHeaderParseResult parseMessageHeader(int msgId, ByteBuf messageHeader) throws Exception {
		if (msgId == NettyMessage.BufferResponse.ID) {
			NettyMessage.BufferResponse<Buffer> bufferResponse =
					NettyMessage.BufferResponse.readFrom(messageHeader, networkBufferAllocator);

			if (bufferResponse.getBuffer() == null) {
				return MessageHeaderParseResult.discardDataBuffer(bufferResponse, bufferResponse.getBufferSize());
			} else {
				return MessageHeaderParseResult.receiveDataBuffer(bufferResponse, bufferResponse.getBufferSize(), bufferResponse.getBuffer());
			}
		} else {
			return super.parseMessageHeader(msgId, messageHeader);
		}
	}
}
