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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.InitialPosition;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardMetadata;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Flink Kinesis Consumer is an exactly-once parallel streaming data source that subscribes to multiple AWS Kinesis
 * streams within the same AWS service region, and can handle resharding of streams. Each subtask of the consumer is
 * responsible for fetching data records from multiple Kinesis shards. The number of shards fetched by each subtask will
 * change as shards are closed and created by Kinesis.
 *
 * <p>To leverage Flink's checkpointing mechanics for exactly-once streaming processing guarantees, the Flink Kinesis
 * consumer is implemented with the AWS Java SDK, instead of the officially recommended AWS Kinesis Client Library, for
 * low-level control on the management of stream state. The Flink Kinesis Connector also supports setting the initial
 * starting points of Kinesis streams, namely TRIM_HORIZON and LATEST.</p>
 *
 * @param <T> the type of data emitted
 */
@PublicEvolving
public class FlinkKinesisConsumer<T> extends RichParallelSourceFunction<T> implements
		ResultTypeQueryable<T>,
		CheckpointedFunction {

	private static final long serialVersionUID = 4724006128720664870L;

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKinesisConsumer.class);

	// ------------------------------------------------------------------------
	//  Consumer properties
	// ------------------------------------------------------------------------

	/** The names of the Kinesis streams that we will be consuming from. */
	private final List<String> streams;

	/** Properties to parametrize settings such as AWS service region, initial position in stream,
	 * shard list retrieval behaviours, etc. */
	private final Properties configProps;

	/** User supplied deserialization schema to convert Kinesis byte messages to Flink objects. */
	private final KinesisDeserializationSchema<T> deserializer;

	// ------------------------------------------------------------------------
	//  Runtime state
	// ------------------------------------------------------------------------

	/** Per-task fetcher for Kinesis data records, where each fetcher pulls data from one or more Kinesis shards. */
	private transient KinesisDataFetcher<T> fetcher;

	/** The sequence numbers to restore to upon restore from failure. */
	private transient HashMap<StreamShardMetadata, SequenceNumber> sequenceNumsToRestore;

	private volatile boolean running = true;

	// ------------------------------------------------------------------------
	//  State for Checkpoint
	// ------------------------------------------------------------------------

	/** State name to access shard sequence number states; cannot be changed. */
	private static final String sequenceNumsStateStoreName = "Kinesis-Stream-Shard-State";

	private transient ListState<Tuple2<StreamShardMetadata, SequenceNumber>> sequenceNumsStateForCheckpoint;

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
		this(stream, new KinesisDeserializationSchemaWrapper<>(deserializer), configProps);
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
		checkArgument(streams.size() != 0, "must be consuming at least 1 stream");
		checkArgument(!streams.contains(""), "stream names cannot be empty Strings");
		this.streams = streams;

		this.configProps = checkNotNull(configProps, "configProps can not be null");

		// check the configuration properties for any conflicting settings
		KinesisConfigUtil.validateConsumerConfiguration(this.configProps);

		checkNotNull(deserializer, "deserializer can not be null");
		checkArgument(
			InstantiationUtil.isSerializable(deserializer),
			"The provided deserialization schema is not serializable: " + deserializer.getClass().getName() + ". " +
				"Please check that it does not contain references to non-serializable instances.");
		this.deserializer = deserializer;

		if (LOG.isInfoEnabled()) {
			StringBuilder sb = new StringBuilder();
			for (String stream : streams) {
				sb.append(stream).append(", ");
			}
			LOG.info("Flink Kinesis Consumer is going to read the following streams: {}", sb.toString());
		}
	}

	// ------------------------------------------------------------------------
	//  Source life cycle
	// ------------------------------------------------------------------------

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {

		// all subtasks will run a fetcher, regardless of whether or not the subtask will initially have
		// shards to subscribe to; fetchers will continuously poll for changes in the shard list, so all subtasks
		// can potentially have new shards to subscribe to later on
		KinesisDataFetcher<T> fetcher = createFetcher(streams, sourceContext, getRuntimeContext(), configProps, deserializer);

		// initial discovery
		List<StreamShardHandle> allShards = fetcher.discoverNewShardsToSubscribe();

		for (StreamShardHandle shard : allShards) {
			StreamShardMetadata kinesisStreamShard = KinesisDataFetcher.convertToStreamShardMetadata(shard);
			if (sequenceNumsToRestore != null) {

				// We need to do this to make sure that a shard that was closed after this restored state was taken will be properly
				// detected and have its sequence numbers restored. A shard will be closed when re-sharding, which can happen when
				// scaling up & down the Kinesis stream, and if the state is not synchronized, then the equality check of the current
				// Kinesis shard will not match the stored state, which will cause us to re-read the entire shard from the event horizon.
				if (updateKinesisShardStateWithMissingEndingSequenceNumber(kinesisStreamShard, sequenceNumsToRestore)) {
					if (LOG.isInfoEnabled()) {
						LOG.info("Updated local stored state for shard {} with a new ending number: {}", kinesisStreamShard.getShardId(), sequenceNumsToRestore.get(kinesisStreamShard));
					}
				}
				if (sequenceNumsToRestore.containsKey(kinesisStreamShard)) {
					// if the shard was already seen and is contained in the state,
					// just use the sequence number stored in the state
					fetcher.registerNewSubscribedShardState(
						new KinesisStreamShardState(kinesisStreamShard, shard, sequenceNumsToRestore.get(kinesisStreamShard)));

					if (LOG.isInfoEnabled()) {
						LOG.info("Subtask {} is seeding the fetcher with restored shard {}," +
								" starting state set to the restored sequence number {}",
							getRuntimeContext().getIndexOfThisSubtask(), shard.toString(), sequenceNumsToRestore.get(kinesisStreamShard));
					}
				} else {
					// the shard wasn't discovered in the previous run, therefore should be consumed from the beginning
					fetcher.registerNewSubscribedShardState(
						new KinesisStreamShardState(kinesisStreamShard, shard, SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get()));

					if (LOG.isInfoEnabled()) {
						LOG.info("Subtask {} is seeding the fetcher with new discovered shard {}," +
								" starting state set to the SENTINEL_EARLIEST_SEQUENCE_NUM",
							getRuntimeContext().getIndexOfThisSubtask(), shard.toString());
					}
				}
			} else {
				// we're starting fresh; use the configured start position as initial state
				SentinelSequenceNumber startingSeqNum =
					InitialPosition.valueOf(configProps.getProperty(
						ConsumerConfigConstants.STREAM_INITIAL_POSITION,
						ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION)).toSentinelSequenceNumber();

				fetcher.registerNewSubscribedShardState(
					new KinesisStreamShardState(kinesisStreamShard, shard, startingSeqNum.get()));

				if (LOG.isInfoEnabled()) {
					LOG.info("Subtask {} will be seeded with initial shard {}, starting state set as sequence number {}",
						getRuntimeContext().getIndexOfThisSubtask(), shard.toString(), startingSeqNum.get());
				}
			}
		}

		// check that we are running before starting the fetcher
		if (!running) {
			return;
		}

		// expose the fetcher from this point, so that state
		// snapshots can be taken from the fetcher's state holders
		this.fetcher = fetcher;

		// start the fetcher loop. The fetcher will stop running only when cancel() or
		// close() is called, or an error is thrown by threads created by the fetcher
		fetcher.runFetcher();

		// check that the fetcher has terminated before fully closing
		fetcher.awaitTermination();
		sourceContext.close();
	}

	/**
	 * Synchronizes the Kinesis shard information from the current Kinesis shard with the restored state, if we find
	 * a shard that match the shardId and streamName. If we find one, and its ending key is different that what we
	 * have in our stored state, then we update the stored's shard's metadata's ending number.
	 *
	 * @param current				the current Kinesis shard we're trying to synchronize.
	 * @param sequenceNumsToRestore	the (re)stored shard metadata and their sequence numbers.
	 * @return {@code true} if the local state was updated with the current Kinesis shard's ending number.
	 */
	@VisibleForTesting
	boolean updateKinesisShardStateWithMissingEndingSequenceNumber(StreamShardMetadata current, HashMap<StreamShardMetadata, SequenceNumber> sequenceNumsToRestore) {
		checkNotNull(current.getStreamName(), "Stream name not set on the current metadata shard");
		checkNotNull(current.getShardId(), "Shard id not set on the current metadata shard");

		// short-circuit: if the current shard doesn't have an ending sequence number, then there's no point in trying to update the local state
		// since that's the only property that can change.
		if (current.getEndingSequenceNumber() == null) {
			return false;
		}

		// try to find the matching shard based on the id & stream name
		for (Map.Entry<StreamShardMetadata, SequenceNumber> entry : sequenceNumsToRestore.entrySet()) {
			if (current.getStreamName().equals(entry.getKey().getStreamName())
				&& current.getShardId().equals(entry.getKey().getShardId())) {
				// synchronize the local state if the ending sequence number is different
				if (!current.getEndingSequenceNumber().equals(entry.getKey().getEndingSequenceNumber())) {
					// ugly, but since the hashcode will change, we'll need to remove it and add it back
					sequenceNumsToRestore.remove(entry.getKey());
					entry.getKey().setEndingSequenceNumber(current.getEndingSequenceNumber());
					sequenceNumsToRestore.put(entry.getKey(), entry.getValue());
					return true;
				}
				// we already found the matching shard
				break;
			}
		}
		return false;
	}

	@Override
	public void cancel() {
		running = false;

		KinesisDataFetcher fetcher = this.fetcher;
		this.fetcher = null;

		// this method might be called before the subtask actually starts running,
		// so we must check if the fetcher is actually created
		if (fetcher != null) {
			try {
				// interrupt the fetcher of any work
				fetcher.shutdownFetcher();
				fetcher.awaitTermination();
			} catch (Exception e) {
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
	public void initializeState(FunctionInitializationContext context) throws Exception {
		TypeInformation<Tuple2<StreamShardMetadata, SequenceNumber>> shardsStateTypeInfo = new TupleTypeInfo<>(
			TypeInformation.of(StreamShardMetadata.class),
			TypeInformation.of(SequenceNumber.class));

		sequenceNumsStateForCheckpoint = context.getOperatorStateStore().getUnionListState(
			new ListStateDescriptor<>(sequenceNumsStateStoreName, shardsStateTypeInfo));

		if (context.isRestored()) {
			if (sequenceNumsToRestore == null) {
				sequenceNumsToRestore = new HashMap<>();
				for (Tuple2<StreamShardMetadata, SequenceNumber> kinesisSequenceNumber : sequenceNumsStateForCheckpoint.get()) {
					sequenceNumsToRestore.put(kinesisSequenceNumber.f0, kinesisSequenceNumber.f1);
				}

				LOG.info("Setting restore state in the FlinkKinesisConsumer. Using the following offsets: {}",
					sequenceNumsToRestore);
			}
		} else {
			LOG.info("No restore state for FlinkKinesisConsumer.");
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (!running) {
			LOG.debug("snapshotState() called on closed source; returning null.");
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Snapshotting state ...");
			}

			sequenceNumsStateForCheckpoint.clear();

			if (fetcher == null) {
				if (sequenceNumsToRestore != null) {
					for (Map.Entry<StreamShardMetadata, SequenceNumber> entry : sequenceNumsToRestore.entrySet()) {
						// sequenceNumsToRestore is the restored global union state;
						// should only snapshot shards that actually belong to us

						if (KinesisDataFetcher.isThisSubtaskShouldSubscribeTo(
								KinesisDataFetcher.convertToStreamShardHandle(entry.getKey()),
								getRuntimeContext().getNumberOfParallelSubtasks(),
								getRuntimeContext().getIndexOfThisSubtask())) {

							sequenceNumsStateForCheckpoint.add(Tuple2.of(entry.getKey(), entry.getValue()));
						}
					}
				}
			} else {
				HashMap<StreamShardMetadata, SequenceNumber> lastStateSnapshot = fetcher.snapshotState();

				if (LOG.isDebugEnabled()) {
					LOG.debug("Snapshotted state, last processed sequence numbers: {}, checkpoint id: {}, timestamp: {}",
						lastStateSnapshot, context.getCheckpointId(), context.getCheckpointTimestamp());
				}

				for (Map.Entry<StreamShardMetadata, SequenceNumber> entry : lastStateSnapshot.entrySet()) {
					sequenceNumsStateForCheckpoint.add(Tuple2.of(entry.getKey(), entry.getValue()));
				}
			}
		}
	}

	/** This method is exposed for tests that need to mock the KinesisDataFetcher in the consumer. */
	protected KinesisDataFetcher<T> createFetcher(
			List<String> streams,
			SourceFunction.SourceContext<T> sourceContext,
			RuntimeContext runtimeContext,
			Properties configProps,
			KinesisDeserializationSchema<T> deserializationSchema) {

		return new KinesisDataFetcher<>(streams, sourceContext, runtimeContext, configProps, deserializationSchema);
	}

	@VisibleForTesting
	HashMap<StreamShardMetadata, SequenceNumber> getRestoredState() {
		return sequenceNumsToRestore;
	}
}
