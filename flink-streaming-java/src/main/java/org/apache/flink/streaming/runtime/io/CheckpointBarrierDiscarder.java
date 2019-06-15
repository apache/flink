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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import java.io.IOException;

/**
 * The {@link CheckpointBarrierDiscarder} discards checkpoint barriers have been received from which input channels.
 */
@Internal
public class CheckpointBarrierDiscarder extends CheckpointBarrierHandler {
	public CheckpointBarrierDiscarder() {
		super(null);
	}

	@Override
	public void releaseBlocksAndResetBarriers() throws IOException {
	}

	@Override
	public boolean isBlocked(int channelIndex) {
		return false;
	}

	@Override
	public boolean processBarrier(CheckpointBarrier receivedBarrier, int channelIndex, long bufferedBytes) throws Exception {
		return false;
	}

	@Override
	public boolean processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception {
		return false;
	}

	@Override
	public boolean processEndOfPartition() throws Exception {
		return false;
	}

	@Override
	public long getLatestCheckpointId() {
		return 0;
	}

	@Override
	public long getAlignmentDurationNanos() {
		return 0;
	}

	@Override
	public void checkpointSizeLimitExceeded(long maxBufferedBytes) throws Exception {

	}
}
