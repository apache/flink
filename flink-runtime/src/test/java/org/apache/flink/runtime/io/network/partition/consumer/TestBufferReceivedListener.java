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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferReceivedListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TestBufferReceivedListener implements BufferReceivedListener {
	private final Map<InputChannelInfo, List<Buffer>> notifiedOnBuffers = new HashMap<>();
	final Map<InputChannelInfo, List<CheckpointBarrier>> notifiedOnBarriers = new HashMap<>();

	@Override
	public void notifyBufferReceived(Buffer buffer, InputChannelInfo channelInfo) {
		notifiedOnBuffers.computeIfAbsent(channelInfo, unused -> new ArrayList<>()).add(buffer);
	}

	@Override
	public void notifyBarrierReceived(CheckpointBarrier barrier, InputChannelInfo channelInfo) {
		notifiedOnBarriers.computeIfAbsent(channelInfo, unused -> new ArrayList<>()).add(barrier);
	}
}
