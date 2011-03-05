/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.taskmanager.checkpointing;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.taskmanager.bytebuffered.NetworkConnectionManager;

public class CheckpointRecoveryThread extends Thread {

	private final NetworkConnectionManager networkConnectionManager;

	private final FileBufferManager fileBufferManager;

	private final ChannelCheckpoint channelCheckpoint;

	public CheckpointRecoveryThread(final NetworkConnectionManager networkConnectionManager,
			final FileBufferManager fileBufferManager, final ChannelCheckpoint channelCheckpoint,
			final ChannelID sourceChannelID) {
		super("CheckpointRecoveryThread for channel " + sourceChannelID);

		this.networkConnectionManager = networkConnectionManager;
		this.fileBufferManager = fileBufferManager;
		this.channelCheckpoint = channelCheckpoint;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		this.channelCheckpoint.recover(this.networkConnectionManager, this.fileBufferManager);
	}
}
