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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import java.io.IOException;

/**
 * Interface of listener for notifying the received data buffer or checkpoint barrier
 * from network channel.
 */
public interface BufferReceivedListener {

	/**
	 * Called whenever an {@link org.apache.flink.runtime.io.network.partition.consumer.InputChannel} receives a
	 * non-barrier buffer.
	 *
	 * <p>The listener is responsible for copying the buffer if it needs to outlive the invocation. It is guaranteed
	 * that no parallel processing of the buffer happens until this callback returns.
	 */
	void notifyBufferReceived(Buffer buffer, InputChannelInfo channelInfo) throws IOException;

	/**
	 * Invoked when an {@link org.apache.flink.runtime.io.network.partition.consumer.InputChannel} receives a
	 * buffer with a {@link CheckpointBarrier}. It is guaranteed that no parallel processing of the buffer happens
	 * until this callback returns.
	 */
	void notifyBarrierReceived(CheckpointBarrier barrier, InputChannelInfo channelInfo) throws IOException;
}
