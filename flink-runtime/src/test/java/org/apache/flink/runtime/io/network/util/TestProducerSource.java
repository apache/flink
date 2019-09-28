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

package org.apache.flink.runtime.io.network.util;

import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

public interface TestProducerSource {

	/**
	 * Returns the next buffer or event instance.
	 *
	 * <p> The channel index specifies the subpartition add the data to.
	 */
	BufferConsumerAndChannel getNextBufferConsumer() throws Exception;

	class BufferConsumerAndChannel {
		private final BufferConsumer bufferConsumer;
		private final int targetChannel;

		public BufferConsumerAndChannel(BufferConsumer bufferConsumer, int targetChannel) {
			this.bufferConsumer = checkNotNull(bufferConsumer);
			this.targetChannel = targetChannel;
		}

		public BufferConsumer getBufferConsumer() {
			return bufferConsumer;
		}

		public int getTargetChannel() {
			return targetChannel;
		}
	}
}
