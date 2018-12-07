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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.net.ProtocolException;

/**
 * The parser for messages with no data buffer part.
 */
public class NoDataBufferMessageParser implements NettyMessageParser {

	@Override
	public int getMessageHeaderLength(int lengthWithoutFrameHeader, int msgId) {
		return lengthWithoutFrameHeader;
	}

	@Override
	public MessageHeaderParseResult parseMessageHeader(int msgId, ByteBuf messageHeader) throws Exception {
		NettyMessage decodedMsg;

		switch (msgId) {
			case NettyMessage.PartitionRequest.ID:
				decodedMsg = NettyMessage.PartitionRequest.readFrom(messageHeader);
				break;
			case NettyMessage.TaskEventRequest.ID:
				decodedMsg = NettyMessage.TaskEventRequest.readFrom(messageHeader, getClass().getClassLoader());
				break;
			case NettyMessage.ErrorResponse.ID:
				decodedMsg = NettyMessage.ErrorResponse.readFrom(messageHeader);
				break;
			case NettyMessage.CancelPartitionRequest.ID:
				decodedMsg = NettyMessage.CancelPartitionRequest.readFrom(messageHeader);
				break;
			case NettyMessage.CloseRequest.ID:
				decodedMsg = NettyMessage.CloseRequest.readFrom(messageHeader);
				break;
			case NettyMessage.AddCredit.ID:
				decodedMsg = NettyMessage.AddCredit.readFrom(messageHeader);
				break;
			default:
				throw new ProtocolException("Received unknown message from producer: " + messageHeader);
		}

		return MessageHeaderParseResult.noDataBuffer(decodedMsg);
	}
}
