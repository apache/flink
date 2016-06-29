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

package org.apache.flink.streaming.connectors.kinesis.internals;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Kinesis Data Fetcher that consumes data from a specific set of Kinesis shards.
 * The fetcher spawns a single thread for connection to each shard.
 */
public class KinesisDataFetcher {

	private static final Logger LOG = LoggerFactory.getLogger(KinesisDataFetcher.class);

	/** Config properties for the Flink Kinesis Consumer */
	private final Properties configProps;

	/** The name of the consumer task that this fetcher was instantiated */
	private final String taskName;

	/** Information of the shards that this fetcher handles, along with the sequence numbers that they should start from */
	private HashMap<KinesisStreamShard, SequenceNumber> assignedShardsWithStartingSequenceNum;

	/** Reference to the thread that executed run() */
	private volatile Thread mainThread;

	/** Reference to the first error thrown by any of the spawned shard connection threads */
	private final AtomicReference<Throwable> error;

	private volatile boolean running = true;

	/**
	 * Creates a new Kinesis Data Fetcher for the specified set of shards
	 *
	 * @param assignedShards the shards that this fetcher will pull data from
	 * @param configProps the configuration properties of this Flink Kinesis Consumer
	 * @param taskName the task name of this consumer task
	 */
	public KinesisDataFetcher(List<KinesisStreamShard> assignedShards, Properties configProps, String taskName) {
		this.configProps = checkNotNull(configProps);
		this.assignedShardsWithStartingSequenceNum = new HashMap<>();
		for (KinesisStreamShard shard : assignedShards) {
			assignedShardsWithStartingSequenceNum.put(shard, SentinelSequenceNumber.SENTINEL_SEQUENCE_NUMBER_NOT_SET.get());
		}
		this.taskName = taskName;
		this.error = new AtomicReference<>();
	}

	/**
	 * Advance a shard's starting sequence number to a specified value
	 *
	 * @param streamShard the shard to perform the advance on
	 * @param sequenceNum the sequence number to advance to
	 */
	public void advanceSequenceNumberTo(KinesisStreamShard streamShard, SequenceNumber sequenceNum) {
		if (!assignedShardsWithStartingSequenceNum.containsKey(streamShard)) {
			throw new IllegalArgumentException("Can't advance sequence number on a shard we are not going to read.");
		}
		assignedShardsWithStartingSequenceNum.put(streamShard, sequenceNum);
	}

	public <T> void run(SourceFunction.SourceContext<T> sourceContext,
						KinesisDeserializationSchema<T> deserializationSchema,
						HashMap<KinesisStreamShard, SequenceNumber> lastSequenceNums) throws Exception {

		if (assignedShardsWithStartingSequenceNum == null || assignedShardsWithStartingSequenceNum.size() == 0) {
			throw new IllegalArgumentException("No shards set to read for this fetcher");
		}

		this.mainThread = Thread.currentThread();

		LOG.info("Reading from shards " + assignedShardsWithStartingSequenceNum);

		// create a thread for each individual shard
		ArrayList<ShardConsumerThread<?>> consumerThreads = new ArrayList<>(assignedShardsWithStartingSequenceNum.size());
		for (Map.Entry<KinesisStreamShard, SequenceNumber> assignedShard : assignedShardsWithStartingSequenceNum.entrySet()) {
			ShardConsumerThread<T> thread = new ShardConsumerThread<>(this, configProps, assignedShard.getKey(),
				assignedShard.getValue(), sourceContext, InstantiationUtil.clone(deserializationSchema), lastSequenceNums);
			thread.setName(String.format("ShardConsumer - %s - %s/%s",
				taskName, assignedShard.getKey().getStreamName() ,assignedShard.getKey().getShardId()));
			thread.setDaemon(true);
			consumerThreads.add(thread);
		}

		// check that we are viable for running for the last time before starting threads
		if (!running) {
			return;
		}

		for (ShardConsumerThread<?> shardConsumer : consumerThreads) {
			LOG.info("Starting thread {}", shardConsumer.getName());
			shardConsumer.start();
		}

		// wait until all consumer threads are done, or until the fetcher is aborted, or until
		// an error occurred in one of the consumer threads
		try {
			boolean consumersStillRunning = true;
			while (running && error.get() == null && consumersStillRunning) {
				try {
					// wait for the consumer threads. if an error occurs, we are interrupted
					for (ShardConsumerThread<?> consumerThread : consumerThreads) {
						consumerThread.join();
					}

					// check if there are consumer threads still running
					consumersStillRunning = false;
					for (ShardConsumerThread<?> consumerThread : consumerThreads) {
						consumersStillRunning = consumersStillRunning | consumerThread.isAlive();
					}
				} catch (InterruptedException e) {
					// ignore
				}
			}

			// make sure any asynchronous error is noticed
			Throwable error = this.error.get();
			if (error != null) {
				throw new Exception(error.getMessage(), error);
			}
		} finally {
			for (ShardConsumerThread<?> consumerThread : consumerThreads) {
				if (consumerThread.isAlive()) {
					consumerThread.cancel();
				}
			}
		}
	}

	public void close() throws IOException {
		this.running = false;
	}

	public void stopWithError(Throwable throwable) {
		if (this.error.compareAndSet(null, throwable)) {
			if (mainThread != null) {
				mainThread.interrupt();
			}
		}
	}
}
