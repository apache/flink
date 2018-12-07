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
 * Responsible for offering the message header length and parsing the message header part of the message.
 */
public interface NettyMessageParser {
	/**
	 * Indicates how to deal with the data buffer part of the current message.
	 */
	enum DataBufferAction {
		/** The current message does not have the data buffer part. */
		NO_DATA_BUFFER,

		/** The current message has data buffer part and this part needs to be received. */
		RECEIVE,

		/**
		 * The current message has data buffer part but it needs to be discarded. For example,
		 * when the target input channel has been closed.
		 */
		DISCARD
	}

	/**
	 * Indicates the result of parsing the message header.
	 */
	class MessageHeaderParseResult {
		/** The message object parsed from the buffer. */
		private NettyMessage parsedMessage;

		/** The target action of how to process the data buffer part. */
		private DataBufferAction dataBufferAction;

		/** The size of the data buffer part. */
		private int dataBufferSize;

		/** The target data buffer for receiving the data. */
		private Buffer targetDataBuffer;

		private MessageHeaderParseResult(
				NettyMessage parsedMessage,
				DataBufferAction dataBufferAction,
				int dataBufferSize,
				Buffer targetDataBuffer) {
			this.parsedMessage = parsedMessage;
			this.dataBufferAction = dataBufferAction;
			this.dataBufferSize = dataBufferSize;
			this.targetDataBuffer = targetDataBuffer;
		}

		static MessageHeaderParseResult noDataBuffer(NettyMessage message) {
			return new MessageHeaderParseResult(message, DataBufferAction.NO_DATA_BUFFER, 0, null);
		}

		static MessageHeaderParseResult receiveDataBuffer(NettyMessage message, int dataBufferSize, Buffer targetDataBuffer) {
			return new MessageHeaderParseResult(message, DataBufferAction.RECEIVE, dataBufferSize, targetDataBuffer);
		}

		static MessageHeaderParseResult discardDataBuffer(NettyMessage message, int dataBufferSize) {
			return new MessageHeaderParseResult(message, DataBufferAction.DISCARD, dataBufferSize, null);
		}

		NettyMessage getParsedMessage() {
			return parsedMessage;
		}

		DataBufferAction getDataBufferAction() {
			return dataBufferAction;
		}

		int getDataBufferSize() {
			return dataBufferSize;
		}

		Buffer getTargetDataBuffer() {
			return targetDataBuffer;
		}
	}

	/**
	 * Get the length of the message header part.
	 *
	 * @param lengthWithoutFrameHeader the message length not counting the frame header part.
	 * @param msgId the id of the current message.
	 * @return the length of the message header part.
	 */
	int getMessageHeaderLength(int lengthWithoutFrameHeader, int msgId);

	/**
	 * Parse the message header.
	 *
	 * @param msgId  the id of the current message.
	 * @param messageHeader the buffer containing the serialized message header.
	 * @return the parsed result.
	 */
	MessageHeaderParseResult parseMessageHeader(int msgId, ByteBuf messageHeader) throws Exception;
}
