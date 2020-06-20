package org.apache.flink.runtime.checkpoint.channel;
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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;

import java.io.IOException;

/**
 * Reads channel state saved during checkpoint/savepoint.
 */
@Internal
public interface ChannelStateReader extends AutoCloseable {

	/**
	 * Status of reading result.
	 */
	enum ReadResult { HAS_MORE_DATA, NO_MORE_DATA }

	/**
	 * Return whether there are any channel states to be read.
	 */
	boolean hasChannelStates();

	/**
	 * Put data into the supplied buffer to be injected into
	 * {@link org.apache.flink.runtime.io.network.partition.consumer.InputChannel InputChannel}.
	 */
	ReadResult readInputData(InputChannelInfo info, Buffer buffer) throws IOException;

	/**
	 * Put data into the supplied buffer to be injected into
	 * {@link org.apache.flink.runtime.io.network.partition.ResultSubpartition ResultSubpartition}.
	 */
	ReadResult readOutputData(ResultSubpartitionInfo info, BufferBuilder bufferBuilder) throws IOException;

	@Override
	void close() throws Exception;

	ChannelStateReader NO_OP = new ChannelStateReader() {

		@Override
		public boolean hasChannelStates() {
			return false;
		}

		@Override
		public ReadResult readInputData(InputChannelInfo info, Buffer buffer) {
			return ReadResult.NO_MORE_DATA;
		}

		@Override
		public ReadResult readOutputData(ResultSubpartitionInfo info, BufferBuilder bufferBuilder) {
			return ReadResult.NO_MORE_DATA;
		}

		@Override
		public void close() {
		}
	};
}
