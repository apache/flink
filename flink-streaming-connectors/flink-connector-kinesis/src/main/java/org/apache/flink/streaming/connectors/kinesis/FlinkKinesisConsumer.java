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

package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kinesis.config.InitialPosition;
import org.apache.flink.streaming.connectors.kinesis.config.KinesisConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Flink Kinesis Consumer is a parallel streaming data source that pulls data from multiple AWS Kinesis streams
 * within the same AWS service region. Each instance of the consumer is responsible for fetching data records from
 * one or more Kinesis shards.
 *
 * <p>To leverage Flink's checkpointing mechanics for exactly-once streaming processing guarantees, the Flink Kinesis
 * consumer is implemented with the AWS Java SDK, instead of the officially recommended AWS Kinesis Client Library, for
 * low-level control on the management of stream state. The Flink Kinesis Connector also supports setting the initial
 * starting points of Kinesis streams, namely TRIM_HORIZON and LATEST.</p>
 *
 * <p><b>NOTE:</b> The current implementation does not correctly handle resharding of AWS Kinesis streams.</p>
 * <p><b>NOTE:</b> Since Kinesis and Kafka share many common abstractions, the implementation is heavily based on
 * the Flink Kafka Consumer.</p>
 *
 * @param <T> the type of data emitted
 */
public class FlinkKinesisConsumer<T> extends RichParallelSourceFunction<T>
	implements CheckpointedAsynchronously<HashMap<KinesisStreamShard, SequenceNumber>>, ResultTypeQueryable<T> {

	private static final long serialVersionUID = 4724006128720664870L;

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKinesisConsumer.class);

	// ------------------------------------------------------------------------
	//  Consumer properties
	// ------------------------------------------------------------------------

	/** The complete list of shards */
	private final List<KinesisStreamShard> shards;

	/** Properties to parametrize settings such as AWS service region, initial position in stream,
	 * shard list retrieval behaviours, etc */
	private final Properties configProps;

	/** User supplied deseriliazation schema to convert Kinesis byte messages to Flink objects */
	private final KinesisDeserializationSchema<T> deserializer;

	// ------------------------------------------------------------------------
	//  Runtime state
	// ------------------------------------------------------------------------

	/** Per-task fetcher for Kinesis data records, where each fetcher pulls data from one or more Kinesis shards */
	private transient KinesisDataFetcher fetcher;

	/** The sequence numbers of the last fetched data records from Kinesis by this task */
	private transient HashMap<KinesisStreamShard, SequenceNumber> lastSequenceNums;

	/** The sequence numbers to restore to upon restore from failure */
	private transient HashMap<KinesisStreamShard, SequenceNumber> sequenceNumsToRestore;

	private volatile boolean hasAssignedShards;

	private volatile boolean running = true;


	// ------------------------------------------------------------------------
	//  Constructors
	// ------------------------------------------------------------------------

	/**
	 * Creates a new Flink Kinesis Consumer.
	 *
	 * <p>The AWS credentials to be used, AWS region of the Kinesis streams, initial position to start streaming
	 * from are configured with a {@link Properties} instance.</p>
	 *
	 * @param stream
	 *           The single AWS Kinesis stream to read from.
	 * @param deserializer
	 *           The deserializer used to convert raw bytes of Kinesis records to Java objects (without key).
	 * @param configProps
	 *           The properties used to configure AWS credentials, AWS region, and initial starting position.
	 */
	public FlinkKinesisConsumer(String stream, DeserializationSchema<T> deserializer, Properties configProps) {
		this(stream, new KinesisDeserializationSchemaWrapper<T>(deserializer), configProps);
	}

	/**
	 * Creates a new Flink Kinesis Consumer.
	 *
	 * <p>The AWS credentials to be used, AWS region of the Kinesis streams, initial position to start streaming
	 * from are configured with a {@link Properties} instance.</p>
	 *
	 * @param stream
	 *           The single AWS Kinesis stream to read from.
	 * @param deserializer
	 *           The keyed deserializer used to convert raw bytes of Kinesis records to Java objects.
	 * @param configProps
	 *           The properties used to configure AWS credentials, AWS region, and initial starting position.
	 */
	public FlinkKinesisConsumer(String stream, KinesisDeserializationSchema<T> deserializer, Properties configProps) {
		this(Collections.singletonList(stream), deserializer, configProps);
	}

	/**
	 * Creates a new Flink Kinesis Consumer.
	 *
	 * <p>The AWS credentials to be used, AWS region of the Kinesis streams, initial position to start streaming
	 * from are configured with a {@link Properties} instance.</p>
	 *
	 * @param streams
	 *           The AWS Kinesis streams to read from.
	 * @param deserializer
	 *           The keyed deserializer used to convert raw bytes of Kinesis records to Java objects.
	 * @param configProps
	 *           The properties used to configure AWS credentials, AWS region, and initial starting position.
	 */
	public FlinkKinesisConsumer(List<String> streams, KinesisDeserializationSchema<T> deserializer, Properties configProps) {
		checkNotNull(streams, "streams can not be null");

		this.configProps = checkNotNull(configProps, "configProps can not be null");

		// check the configuration properties for any conflicting settings
		KinesisConfigUtil.validateConfiguration(this.configProps);

		this.deserializer = checkNotNull(deserializer, "deserializer can not be null");

		this.shards = new KinesisProxy(configProps).getShardList(streams);
		if (shards.size() == 0) {
			throw new RuntimeException("Unable to retrieve any shards for the requested streams " + streams.toString() + ".");
		}

		if (LOG.isInfoEnabled()) {
			Map<String, Integer> shardCountPerStream = new HashMap<>();
			for (KinesisStreamShard shard : shards) {
				Integer shardCount = shardCountPerStream.get(shard.getStreamName());
				if (shardCount == null) {
					shardCount = 1;
				} else {
					shardCount++;
				}
				shardCountPerStream.put(shard.getStreamName(), shardCount);
			}
			StringBuilder sb = new StringBuilder();
			for (Map.Entry<String,Integer> streamAndShardCountPair : shardCountPerStream.entrySet()) {
				sb.append(streamAndShardCountPair.getKey()).append(" (").append(streamAndShardCountPair.getValue()).append("), ");
			}
			LOG.info("Flink Kinesis Consumer is going to read the following streams (with number of shards): {}", sb.toString());
		}
	}

	// ------------------------------------------------------------------------
	//  Source life cycle
	// ------------------------------------------------------------------------

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		final int numFlinkConsumerTasks = getRuntimeContext().getNumberOfParallelSubtasks();
		final int thisConsumerTaskIndex = getRuntimeContext().getIndexOfThisSubtask();

		// pick which shards this consumer task works on, in a round-robin fashion
		List<KinesisStreamShard> assignedShards = assignShards(this.shards, numFlinkConsumerTasks, thisConsumerTaskIndex);

		// if there are no shards assigned to this consumer task, return without doing anything.
		if (assignedShards.isEmpty()) {
			LOG.info("Consumer task {} has no shards assigned to it", thisConsumerTaskIndex);
			hasAssignedShards = false;
			return;
		} else {
			hasAssignedShards = true;
		}

		if (LOG.isInfoEnabled()) {
			StringBuilder sb = new StringBuilder();
			for (KinesisStreamShard shard : assignedShards) {
				sb.append(shard.getStreamName()).append(":").append(shard.getShardId()).append(", ");
			}
			LOG.info("Consumer task {} will read shards {} out of a total of {} shards",
				thisConsumerTaskIndex, sb.toString(), this.shards.size());
		}

		fetcher = new KinesisDataFetcher(assignedShards, configProps, getRuntimeContext().getTaskName());

		// restore to the last known sequence numbers from the latest complete snapshot
		if (sequenceNumsToRestore != null) {
			if (LOG.isInfoEnabled()) {
				LOG.info("Consumer task {} is restoring sequence numbers from previous checkpointed state", thisConsumerTaskIndex);
			}

			for (Map.Entry<KinesisStreamShard, SequenceNumber> restoreSequenceNum : sequenceNumsToRestore.entrySet()) {
				// advance the corresponding shard to the last known sequence number
				fetcher.advanceSequenceNumberTo(restoreSequenceNum.getKey(), restoreSequenceNum.getValue());
			}

			if (LOG.isInfoEnabled()) {
				StringBuilder sb = new StringBuilder();
				for (Map.Entry<KinesisStreamShard, SequenceNumber> restoreSequenceNo : sequenceNumsToRestore.entrySet()) {
					KinesisStreamShard shard = restoreSequenceNo.getKey();
					sb.append(shard.getStreamName()).append(":").append(shard.getShardId())
						.append(" -> ").append(restoreSequenceNo.getValue()).append(", ");
				}
				LOG.info("Advanced the starting sequence numbers of consumer task {}: {}", thisConsumerTaskIndex, sb.toString());
			}

			// initialize sequence numbers with restored state
			lastSequenceNums = sequenceNumsToRestore;
			sequenceNumsToRestore = null;
		} else {
			// start fresh with empty sequence numbers if there are no snapshots to restore from.
			lastSequenceNums = new HashMap<>();

			// advance all assigned shards of this consumer task to either the earliest or latest sequence number,
			// depending on the properties configuration (default is to set to latest sequence number).
			InitialPosition initialPosition = InitialPosition.valueOf(configProps.getProperty(
				KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, InitialPosition.LATEST.toString()));

			SentinelSequenceNumber sentinelSequenceNum;
			switch (initialPosition) {
				case TRIM_HORIZON:
					sentinelSequenceNum = SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM;
					break;
				case LATEST:
				default:
					sentinelSequenceNum = SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM;
			}

			for (KinesisStreamShard assignedShard : assignedShards) {
				fetcher.advanceSequenceNumberTo(assignedShard, sentinelSequenceNum.get());
			}

			if (LOG.isInfoEnabled()) {
				StringBuilder sb = new StringBuilder();
				for (KinesisStreamShard assignedShard : assignedShards) {
					sb.append(assignedShard.getStreamName()).append(":").append(assignedShard.getShardId())
						.append(" -> ").append(sentinelSequenceNum.get()).append(", ");
				}
				LOG.info("Advanced the starting sequence numbers of consumer task {}: {}", thisConsumerTaskIndex, sb.toString());
			}
		}
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {
		if (hasAssignedShards) {
			fetcher.run(sourceContext, deserializer, lastSequenceNums);
		} else {
			// this source never completes because there is no assigned shards,
			// so emit a Long.MAX_VALUE watermark to no block watermark forwarding
			sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));

			final Object waitLock = new Object();
			while (running) {
				try {
					synchronized (waitLock) {
						waitLock.wait();
					}
				} catch (InterruptedException e) {
					// do nothing
				}
			}
		}

		sourceContext.close();
	}

	@Override
	public void cancel() {
		running = false;

		// interrupt the fetcher of any work
		KinesisDataFetcher fetcher = this.fetcher;
		this.fetcher = null;
		if (fetcher != null) {
			try {
				fetcher.close();
			} catch (IOException e) {
				LOG.warn("Error while closing Kinesis data fetcher", e);
			}
		}
	}

	@Override
	public void close() throws Exception {
		cancel();
		super.close();
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return deserializer.getProducedType();
	}

	// ------------------------------------------------------------------------
	//  State Snapshot & Restore
	// ------------------------------------------------------------------------

	@Override
	public HashMap<KinesisStreamShard, SequenceNumber> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		if (lastSequenceNums == null) {
			LOG.debug("snapshotState() requested on not yet opened source; returning null.");
			return null;
		}

		if (!running) {
			LOG.debug("snapshotState() called on closed source; returning null.");
			return null;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Snapshotting state. ...");
		}

		@SuppressWarnings("unchecked")
		HashMap<KinesisStreamShard, SequenceNumber> currentSequenceNums =
			(HashMap<KinesisStreamShard, SequenceNumber>) lastSequenceNums.clone();

		return currentSequenceNums;
	}

	@Override
	public void restoreState(HashMap<KinesisStreamShard, SequenceNumber> restoredState) throws Exception {
		sequenceNumsToRestore = restoredState;
	}

	// ------------------------------------------------------------------------
	//  Miscellaneous utilities
	// ------------------------------------------------------------------------

	/**
	 * Utility function to assign shards to a specific consumer task in a round-robin fashion.
	 */
	protected static List<KinesisStreamShard> assignShards(List<KinesisStreamShard> shards, int numFlinkConsumerTasks, int thisConsumerTaskIndex) {
		checkArgument(numFlinkConsumerTasks > 0);
		checkArgument(thisConsumerTaskIndex < numFlinkConsumerTasks);

		List<KinesisStreamShard> closedShards = new ArrayList<>();
		List<KinesisStreamShard> openShards = new ArrayList<>();

		for (KinesisStreamShard shard : shards) {
			if (shard.isClosed()) {
				closedShards.add(shard);
			} else {
				openShards.add(shard);
			}
		}

		List<KinesisStreamShard> subscribedShards = new ArrayList<>();

		// separately round-robin assign open and closed shards so that all tasks have a fair chance of being
		// assigned open shards (set of data records in closed shards are bounded)

		for (int i = 0; i < closedShards.size(); i++) {
			if (i % numFlinkConsumerTasks == thisConsumerTaskIndex) {
				subscribedShards.add(closedShards.get(i));
			}
		}

		for (int i = 0; i < openShards.size(); i++) {
			if (i % numFlinkConsumerTasks == thisConsumerTaskIndex) {
				subscribedShards.add(openShards.get(i));
			}
		}
		return subscribedShards;
	}
}
