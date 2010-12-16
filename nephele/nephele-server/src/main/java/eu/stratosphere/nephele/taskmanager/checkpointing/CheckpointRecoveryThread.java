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

import java.io.EOFException;
import java.io.IOException;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ByteBufferedChannelManager;

public class CheckpointRecoveryThread extends Thread {

	private final ByteBufferedChannelManager byteBufferedChannelManager;

	private final ChannelCheckpoint channelCheckpoint;

	public CheckpointRecoveryThread(ByteBufferedChannelManager byteBufferedChannelManager,
			ChannelCheckpoint channelCheckpoint, ChannelID sourceChannelID) {
		super("CheckpointRecoveryThread for channel " + sourceChannelID);

		this.byteBufferedChannelManager = byteBufferedChannelManager;
		this.channelCheckpoint = channelCheckpoint;
	}

	@Override
	public void run() {

		try {

			this.channelCheckpoint.recover(this.byteBufferedChannelManager);
		} catch (IOException ioe) {
			ioe.printStackTrace(); // TODO: Handle this correctly
		}
	}
}
