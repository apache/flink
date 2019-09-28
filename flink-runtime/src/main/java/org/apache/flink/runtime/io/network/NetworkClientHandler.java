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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

import java.io.IOException;

/**
 * Channel handler to read and write network messages on client side.
 */
public interface NetworkClientHandler extends ChannelHandler {

	void addInputChannel(RemoteInputChannel inputChannel) throws IOException;

	void removeInputChannel(RemoteInputChannel inputChannel);

	void cancelRequestFor(InputChannelID inputChannelId);

	/**
	 * The credit begins to announce after receiving the sender's backlog from buffer response.
	 * Than means it should only happen after some interactions with the channel to make sure
	 * the context will not be null.
	 *
	 * @param inputChannel The input channel with unannounced credits.
	 */
	void notifyCreditAvailable(final RemoteInputChannel inputChannel);
}
