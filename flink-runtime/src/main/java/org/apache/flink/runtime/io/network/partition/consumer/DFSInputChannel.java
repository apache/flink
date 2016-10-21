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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.metrics.groups.IOMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input channel, which requests data from dfs file.
 */
public class DFSInputChannel extends InputChannel {

	private static final Logger LOG = LoggerFactory.getLogger(DFSInputChannel.class);

	/** ID to distinguish this channel from other channels. */
	private final InputChannelID id = new InputChannelID();

	/** The reader used for read dfs file. */
	private final DFSFileReader dfsReader;

	/**
	 * The received buffers. Received buffers are enqueued by the read thread in DFSFileReader and the queue
	 * is consumed by the receiving task thread.
	 */
	private final Queue<Buffer> receivedBuffers = new ArrayDeque<Buffer>();

	/**
	 * Flag indicating whether this channel has been released. Either called by the receiving task
	 * thread or the task manager actor.
	 */
	private final AtomicBoolean isReleased = new AtomicBoolean();

	/**
	 * the jobId, for generate HDFS file name
	 */
	private JobID jobID;

	public DFSInputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartitionID partitionId,
			JobID jobID,
			IOMetricGroup metrics) {

		this(inputGate, channelIndex, partitionId, jobID,
				new Tuple2<Integer, Integer>(0, 0), metrics);
	}

	public DFSInputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartitionID partitionId,
			JobID jobID,
			Tuple2<Integer, Integer> initialAndMaxBackoff,
			IOMetricGroup metrics) {

		super(inputGate, channelIndex, partitionId, initialAndMaxBackoff, metrics.getNumBytesInRemoteCounter());
		this.jobID = jobID;
		dfsReader = new DFSFileReader(this, jobID.toString() + "_" + partitionId.getPartitionId().toString());
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	/**
	 * Requests a dfs subpartition.
	 */
	@Override
	void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {
		if (!dfsReader.isOpen()) {
			// dfsReader begin to read the partition file
			dfsReader.requestSubpartition(subpartitionIndex);
		}
	}

	/**
	 * Retriggers a dfs subpartition request.
	 */
	void retriggerSubpartitionRequest(int subpartitionIndex) throws IOException, InterruptedException {
		//do nothing
	}

	@Override
	Buffer getNextBuffer() throws IOException {
		checkState(!isReleased.get(), "Queried for a buffer after channel has been closed.");

		checkError();

		synchronized (receivedBuffers) {
			Buffer buffer = receivedBuffers.poll();

			// Sanity check that channel is only queried after a notification
			if (buffer == null) {
				throw new IOException("Queried input channel for data although non is available.");
			}

			numBytesIn.inc(buffer.getSize());
			return buffer;
		}
	}

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	@Override
	void sendTaskEvent(TaskEvent event) throws IOException {
		// Nothing to do
	}

	// ------------------------------------------------------------------------
	// Life cycle
	// ------------------------------------------------------------------------

	@Override
	boolean isReleased() {
		return isReleased.get();
	}

	@Override
	void notifySubpartitionConsumed() {
		// Nothing to do
	}

	/**
	 * Releases all received buffers and closes the dfsReader
	 */
	@Override
	void releaseAllResources() throws IOException {
		if (isReleased.compareAndSet(false, true)) {
			synchronized (receivedBuffers) {
				Buffer buffer;
				while ((buffer = receivedBuffers.poll()) != null) {
					buffer.recycle();
				}
			}

			// The released flag has to be set before closing the connection to ensure that
			// buffers received concurrently with closing are properly recycled.
			if (dfsReader != null) {
				dfsReader.close();
			}
		}
	}

	public void failPartitionRequest() {
		setError(new PartitionNotFoundException(partitionId));
	}

	@Override
	public String toString() {
		return "DFSInputChannel [" + partitionId + ", numberOfQueuedBuffers " + getNumberOfQueuedBuffers() + "]";
	}

	// ------------------------------------------------------------------------
	// Network I/O notifications (called by network I/O thread)
	// ------------------------------------------------------------------------

	public int getNumberOfQueuedBuffers() {
		synchronized (receivedBuffers) {
			return receivedBuffers.size();
		}
	}

	public InputChannelID getInputChannelId() {
		return id;
	}

	public BufferProvider getBufferProvider() {
		if (isReleased.get()) {
			return null;
		}

		return inputGate.getBufferProvider();
	}

	public void onBuffer(Buffer buffer) {
		boolean success = false;

		try {
			synchronized (receivedBuffers) {
				if (!isReleased.get()) {
					receivedBuffers.add(buffer);

					notifyAvailableBuffer();

					success = true;
				}
			}
		}
		finally {
			if (!success) {
				buffer.recycle();
			}
		}
	}

	public void onError(Throwable cause) {
		setError(cause);
	}

}
