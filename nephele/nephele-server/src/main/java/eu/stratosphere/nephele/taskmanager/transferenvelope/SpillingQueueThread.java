/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.util.StringUtils;

public final class SpillingQueueThread extends Thread {

	private static final Log LOG = LogFactory.getLog(SpillingQueueThread.class);

	private final Object monitorObject = new Object();

	private boolean firstLockAcquired = false;

	private final BufferProvider bufferProvider;

	private final SpillingQueueElement startElem;

	private final SpillingQueue spillingQueue;

	private final int unspillLimit;

	SpillingQueueThread(final BufferProvider bufferProvider, final SpillingQueueElement startElem,
			final SpillingQueue spillingQueue) {

		this.bufferProvider = bufferProvider;
		this.startElem = startElem;
		this.spillingQueue = spillingQueue;
		this.unspillLimit = 4 * 1024 * 1024; // 4MB
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		SpillingQueueElement elem = this.startElem;
		if (elem == null) {
			LOG.error("SpillingQueueThread has been started with startElem == null");
			return;
		}

		int usedMemory = 0;

		while (elem != null) {

			synchronized (elem) {

				synchronized (this.monitorObject) {
					if (!this.firstLockAcquired) {
						this.firstLockAcquired = true;
						this.monitorObject.notify();
					}
				}

				try {
					usedMemory += elem.unspill(this.bufferProvider);
				} catch (IOException ioe) {
					LOG.error(StringUtils.stringifyException(ioe));
				}

				if (usedMemory >= this.unspillLimit) {
					this.spillingQueue.increaseAmountOfMainMemoryInQueue(usedMemory);
					return;
				}
			}

			elem = elem.getNextElement();
		}

		this.spillingQueue.increaseAmountOfMainMemoryInQueue(usedMemory);
	}

	void waitUntilFirstLockIsAcquired() {

		try {

			synchronized (this.monitorObject) {

				while (!this.firstLockAcquired) {

					this.monitorObject.wait();
				}
			}
		} catch (InterruptedException e) {
			LOG.error(StringUtils.stringifyException(e));
		}
	}
}
