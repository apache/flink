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

package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.Iterator;
import java.util.Queue;

public class ReadBufferDumpThread extends Thread {

	private final Queue<TransferEnvelope> queueOfTransferEnvelopes;

	//private final Deque<ByteBuffer> emptyReadBuffers;

	public ReadBufferDumpThread(Queue<TransferEnvelope> queueOfTransferEnvelopes, Deque<ByteBuffer> emptyReadBuffers) {
		this.queueOfTransferEnvelopes = queueOfTransferEnvelopes;
		//this.emptyReadBuffers = emptyReadBuffers;
	}

	@Override
	public void run() {

		synchronized (this.queueOfTransferEnvelopes) {

			if (this.queueOfTransferEnvelopes.isEmpty()) {
				return;
			}

			//final TransferEnvelope queueHead = this.queueOfTransferEnvelopes.peek();
			final Iterator<TransferEnvelope> it = this.queueOfTransferEnvelopes.iterator();

			while (it.hasNext()) {

				it.next();
				//final TransferEnvelope transferEnvelope = it.next();

				/*
				 * Make sure not to touch the queue head as this envelope may already
				 * be processed by the respective input channel.
				 */
				// TODO: Fix me
				/*
				 * if(transferEnvelope != queueHead) {
				 * final ReadBuffer readBuffer= transferEnvelope.getBuffer();
				 * if(readBuffer != null) {
				 * try {
				 * readBuffer.dumpToDisk(this.emptyReadBuffers);
				 * } catch(IOException ioe) {
				 * //TODO: Handle this properly
				 * ioe.printStackTrace();
				 * }
				 * }
				 * }
				 */
			}
		}

	}
}
