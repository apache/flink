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

package org.apache.flink.test.benchmark.network;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Network throughput benchmarks executed by the external
 * <a href="https://github.com/dataArtisans/flink-benchmarks">flink-benchmarks</a> project.
 */
public class StreamNetworkThroughputBenchmark<T extends IOReadableWritable> {
	protected StreamNetworkBenchmarkEnvironment<T> environment;
	protected ReceiverThread<T>[] receiverThreads;
	protected RecordWriterThread<T>[] writerThreads;
	protected int[][] channelRanges;
	protected TypeSerializer<T> serializer;
	protected T[] recordsSet;
	protected T value;
	protected boolean broadcastMode;
	protected int numRecordWriters;
	protected int numRecordReceivers;
	protected int flushTimeout;
	protected int numChannels;
	protected long numRecordsPerWriter;
	protected long[] numRecordToReceive;
	protected CompletableFuture<?>[] receiverFutures;

	public void executeBenchmark() throws Exception {
		executeBenchmark(Long.MAX_VALUE);
	}

	/**
	 * Executes the throughput benchmark with the given number of records.
	 */
	public void executeBenchmark(long timeout) throws Exception {
		for (RecordWriterThread writerThread : writerThreads) {
			writerThread.setRecordsToSend(numRecordsPerWriter);
		}

		for (int i = 0; i < numRecordReceivers; ++i) {
			receiverFutures[i] = receiverThreads[i].setExpectedRecord(numRecordToReceive[i]);
		}

		for (int i = 0; i < numRecordReceivers; ++i) {
			receiverFutures[i].get(timeout, TimeUnit.MILLISECONDS);
		}
	}

	public void setUp(
		int recordWriters,
		int recordReceivers,
		int channels,
		int flushTimeout,
		long numRecordToSend,
		T[] recordsSet,
		T value,
		TypeSerializer<T> serializer) throws Exception {
		setUp(
			recordWriters,
			recordReceivers,
			channels,
			flushTimeout,
			numRecordToSend,
			false,
			recordsSet,
			value,
			serializer);
	}

	public void setUp(
		int recordWriters,
		int recordReceivers,
		int channels,
		int flushTimeout,
		long numRecordToSend,
		boolean localMode,
		T[] recordsSet,
		T value,
		TypeSerializer<T> serializer) throws Exception {
		setUp(
			recordWriters,
			recordReceivers,
			channels,
			flushTimeout,
			numRecordToSend,
			localMode,
			-1,
			-1,
			recordsSet,
			value,
			serializer);
	}

	public void setUp(
			int recordWriters,
			int recordReceivers,
			int channels,
			int flushTimeout,
			long numRecordToSend,
			boolean localMode,
			int senderBufferPoolSize,
			int receiverBufferPoolSize,
			T[] recordsSet,
			T value,
			TypeSerializer<T> serializer) throws Exception {
		setUp(
			recordWriters,
			recordReceivers,
			channels,
			flushTimeout,
			numRecordToSend,
			false,
			localMode,
			senderBufferPoolSize,
			receiverBufferPoolSize,
			recordsSet,
			value,
			serializer);
	}

	public void setUp(
		int recordWriters,
		int recordReceivers,
		int channels,
		int flushTimeout,
		long numRecordToSend,
		boolean broadcastMode,
		boolean localMode,
		int senderBufferPoolSize,
		int receiverBufferPoolSize,
		T[] recordsSet,
		T value,
		TypeSerializer<T> serializer) throws Exception {
		setUp(
			recordWriters,
			recordReceivers,
			channels,
			flushTimeout,
			numRecordToSend,
			broadcastMode,
			localMode,
			senderBufferPoolSize,
			receiverBufferPoolSize,
			recordsSet,
			value,
			serializer,
			new Configuration());
	}

	/**
	 * Initializes the throughput benchmark with the given parameters.
	 *
	 * @param recordWriters
	 * 		number of senders, i.e.
	 * 		{@link org.apache.flink.runtime.io.network.api.writer.RecordWriter} instances
	 * @param recordReceivers number of receiver thread.
	 * @param channels
	 * 		number of outgoing channels / receivers
	 * @param flushTimeout the record flush timeout
	 * @param numRecordToSend number of records to send
	 * @param broadcastMode whether broadcasting the record to all channels
	 * @param localMode whether using local input channel
	 * @param senderBufferPoolSize the size of sender network buffer pool
	 * @param receiverBufferPoolSize the size of receiver network buffer pool
	 * @param recordsSet the records to be sent
	 * @param value the value wrapper to be used by record receiver
	 * @param serializer the type serializer
	 * @param configuration the configuration
	 */
	public void setUp(
			int recordWriters,
			int recordReceivers,
			int channels,
			int flushTimeout,
			long numRecordToSend,
			boolean broadcastMode,
			boolean localMode,
			int senderBufferPoolSize,
			int receiverBufferPoolSize,
			T[] recordsSet,
			T value,
			TypeSerializer<T> serializer,
			Configuration configuration) throws Exception {
		this.serializer = checkNotNull(serializer);
		this.recordsSet = checkNotNull(recordsSet);
		this.value = checkNotNull(value);
		this.broadcastMode = broadcastMode;
		this.numRecordWriters = recordWriters;
		this.numRecordReceivers = Math.min(recordReceivers, channels);
		this.numChannels = channels;
		this.flushTimeout = flushTimeout;
		this.numRecordToReceive = new long[recordReceivers];
		this.receiverFutures = new CompletableFuture[recordReceivers];
		this.numRecordsPerWriter = numRecordToSend / recordWriters;

		distributeChannelsToReceivers();
		calculateRecordToReceive();

		environment = new StreamNetworkBenchmarkEnvironment<>();
		environment.setUp(
			recordWriters,
			channels,
			broadcastMode,
			localMode,
			senderBufferPoolSize,
			receiverBufferPoolSize,
			configuration);

		createWriterThread();
		createRcordWriter();

		createReceiverThread();
		createRcordReceiver();
	}

	protected void createWriterThread() {
		writerThreads = new RecordWriterThread[numRecordWriters];
		for (int writer = 0; writer < numRecordWriters; writer++) {
			writerThreads[writer] = new RecordWriterThread<>(broadcastMode, recordsSet);
			writerThreads[writer].start();
		}
	}

	protected void createReceiverThread() throws IOException {
		receiverThreads = new ReceiverThread[numRecordReceivers];
		for (int receiver = 0; receiver < numRecordReceivers; ++receiver) {
			receiverThreads[receiver] = new ReceiverThread<>(value);
			receiverThreads[receiver].start();
		}
	}

	protected void createRcordWriter() throws Exception {
		for (int writer = 0; writer < numRecordWriters; writer++) {
			ResultPartition<T> resultPartition = environment.createInternalResultPartition(writer, serializer);
			StreamRecordWriter<T> recordWriter = environment.createRecordWriter(resultPartition, flushTimeout);
			writerThreads[writer].setupWriter(recordWriter, resultPartition);
		}
	}

	protected void createRcordReceiver() throws Exception {
		for (int receiver = 0; receiver < numRecordReceivers; ++receiver) {
			SingleInputGate[] inputGates = environment.createInputGate(
				ResultPartitionType.PIPELINED, channelRanges[receiver][0], channelRanges[receiver][1]);
			receiverThreads[receiver].setupReader(inputGates);
		}
	}

	/**
	 * Shuts down a benchmark previously set up via {@link #setUp}.
	 *
	 * <p>This will wait for all senders to finish but timeout with an exception after 5 seconds.
	 */
	public void tearDown() throws Exception {
		for (RecordWriterThread writerThread : writerThreads) {
			writerThread.shutdown();
			writerThread.sync(5000);
		}

		environment.tearDown();

		for (ReceiverThread receiverThread: receiverThreads) {
			receiverThread.shutdown();
			receiverThread.sync(5000);
		}
	}

	protected void distributeChannelsToReceivers() {
		channelRanges = new int[numRecordReceivers][2];
		int numChannelPerReceiver = numChannels / numRecordReceivers;
		int remainingChannels = numChannels % numRecordReceivers;
		int channelIndex = 0;
		for (int i = 0; i < numRecordReceivers; ++i) {
			channelRanges[i] = new int[2];
			channelRanges[i][0] = channelIndex;
			if (i < remainingChannels) {
				channelRanges[i][1] = channelIndex + numChannelPerReceiver + 1;
			} else {
				channelRanges[i][1] = channelIndex + numChannelPerReceiver;
			}
			channelIndex = channelRanges[i][1];
		}
	}

	protected void calculateRecordToReceive() {
		for (int i = 0; i < numRecordReceivers; ++i) {
			if (broadcastMode) {
				numRecordToReceive[i] = numRecordsPerWriter * (channelRanges[i][1] - channelRanges[i][0]) * numRecordWriters;
			} else {
				long numRecordsPerChannel = numRecordsPerWriter / numChannels;
				long remainingRecords = numRecordsPerWriter % numChannels;
				for (int j = channelRanges[i][0]; j < channelRanges[i][1]; ++j) {
					if (j < remainingRecords) {
						numRecordToReceive[i] += (numRecordsPerChannel + 1) * numRecordWriters;
					} else {
						numRecordToReceive[i] += numRecordsPerChannel * numRecordWriters;
					}
				}
			}
		}
	}
}
