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

package eu.stratosphere.nephele.taskmanager.transferenvelope;

import eu.stratosphere.nephele.io.channels.Buffer;

public class TransferEnvelopeProcessingLog {

	private Buffer buffer;

	private final boolean mustBeSentViaNetwork;

	private final boolean mustBeWrittenToCheckpoint;

	private boolean sentViaNetwork = false;

	private boolean writtenToCheckpoint = false;

	public TransferEnvelopeProcessingLog(boolean mustBeSentViaNetwork, boolean mustBeWrittenToCheckpoint) {
		this.mustBeSentViaNetwork = mustBeSentViaNetwork;
		this.mustBeWrittenToCheckpoint = mustBeWrittenToCheckpoint;
	}

	public boolean mustBeSentViaNetwork() {
		return this.mustBeSentViaNetwork;
	}

	public boolean mustBeWrittenToCheckpoint() {
		return this.mustBeWrittenToCheckpoint;
	}

	public synchronized void setBuffer(Buffer buffer) {
		this.buffer = buffer;
	}

	private void recycleByteBufferIfPossible() {

		if (this.buffer == null) {
			return;
		}

		if (this.mustBeSentViaNetwork && !this.sentViaNetwork) {
			return;
		}

		if (this.mustBeWrittenToCheckpoint && !this.writtenToCheckpoint) {
			return;
		}

		// Recycle buffer
		this.buffer.recycleBuffer();
	}

	public synchronized boolean isSentViaNetwork() {
		return this.sentViaNetwork;
	}

	public synchronized boolean isWrittenToCheckpoint() {
		return this.writtenToCheckpoint;
	}

	public synchronized void setSentViaNetwork() {
		this.sentViaNetwork = true;

		recycleByteBufferIfPossible();
	}

	public synchronized void setWrittenToCheckpoint() {
		this.writtenToCheckpoint = true;

		recycleByteBufferIfPossible();
	}
}
