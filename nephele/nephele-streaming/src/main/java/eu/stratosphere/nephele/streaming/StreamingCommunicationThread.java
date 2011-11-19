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

package eu.stratosphere.nephele.streaming;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.plugins.PluginCommunication;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class implements a communication thread to handle communication from the task manager plugin component to the
 * job manager plugin component in an asynchronous fashion. The main reason for asynchronous communication is not
 * influence the processing delay by the RPC call latency.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
final class StreamingCommunicationThread extends Thread {

	/**
	 * The log object.
	 */
	private static final Log LOG = LogFactory.getLog(StreamingCommunicationThread.class);

	/**
	 * The capacity of the data queue.
	 */
	private static final int QUEUE_CAPACITY = 128;

	/**
	 * Stub object representing the job manager component of this plugin.
	 */
	private final PluginCommunication jobManagerComponent;

	/**
	 * The blocking queue which is used to asynchronously exchange data with the job manager component of this plugin.
	 */
	private final BlockingQueue<AbstractStreamingData> dataQueue = new ArrayBlockingQueue<AbstractStreamingData>(QUEUE_CAPACITY);

	/**
	 * Stores whether the communication thread has been requested to stop.
	 */
	private volatile boolean interrupted = false;

	/**
	 * Constructs a new streaming communication thread.
	 * 
	 * @param jobManagerComponent
	 *        the stub object for the plugin's job manager component.
	 */
	StreamingCommunicationThread(final PluginCommunication jobManagerComponent) {
		this.jobManagerComponent = jobManagerComponent;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		while (!this.interrupted) {

			if (Thread.currentThread().isInterrupted()) {
				break;
			}

			try {
				this.jobManagerComponent.sendData(this.dataQueue.take());
			} catch (InterruptedException e) {
				break;
			} catch (IOException ioe) {
				LOG.error(StringUtils.stringifyException(ioe));
			}
		}
	}

	/**
	 * Stops the communication thread.
	 */
	void stopCommunicationThread() {
		this.interrupted = true;
		interrupt();
	}

	/**
	 * Sends the given data item asynchronously to the plugin's job manager component.
	 * 
	 * @param data
	 *        the data item to send to the plugin's job manager component
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the communication thread to accept the data
	 */
	void sendDataAsynchronously(final AbstractStreamingData data) throws InterruptedException {

		this.dataQueue.put(data);
	}
}
