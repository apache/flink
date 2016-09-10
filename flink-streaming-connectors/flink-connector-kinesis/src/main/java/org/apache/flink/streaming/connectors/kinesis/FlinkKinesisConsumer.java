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
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
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
public class FlinkKinesisConsumer<T> extends RichParallelSourceFunction<T>
	implements CheckpointedAsynchronously<HashMap<KinesisStreamShard, SequenceNumber>>, ResultTypeQueryable<T> {

	private static final long serialVersionUID = 4724006128720664870L;

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKinesisConsumer.class);

	// ------------------------------------------------------------------------
	//  Consumer properties
	// ------------------------------------------------------------------------

	/** The names of the Kinesis streams that we will be consuming from */
	private final List<String> streams;

	/** Properties to parametrize settings such as AWS service region, initial position in stream,
	 * shard list retrieval behaviours, etc */
	private final Properties configProps;

	/** User supplied deseriliazation schema to convert Kinesis byte messages to Flink objects */
	private final KinesisDeserializationSchema<T> deserializer;

	// ------------------------------------------------------------------------
	//  Runtime state
	// ------------------------------------------------------------------------

	/** Per-task fetcher for Kinesis data records, where each fetcher pulls data from one or more Kinesis shards */
	private transient KinesisDataFetcher<T> fetcher;

	/** The sequence numbers in the last state snapshot of this subtask */
	private transient HashMap<KinesisStreamShard, SequenceNumber> lastStateSnapshot;

	/** The sequence numbers to restore to upon restore from failure */
	private transient HashMap<KinesisStreamShard, SequenceNumber> sequenceNumsToRestore;

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

		this.deserializer = checkNotNull(deserializer, "deserializer can not be null");

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
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		// restore to the last known sequence numbers from the latest complete snapshot
		if (sequenceNumsToRestore != null) {
			if (LOG.isInfoEnabled()) {
				LOG.info("Subtask {} is restoring sequence numbers {} from previous checkpointed state",
					getRuntimeContext().getIndexOfThisSubtask(), sequenceNumsToRestore.toString());
			}

			// initialize sequence numbers with restored state
			lastStateSnapshot = sequenceNumsToRestore;
		} else {
			// start fresh with empty sequence numbers if there are no snapshots to restore from.
			lastStateSnapshot = new HashMap<>();
		}
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {

		// all subtasks will run a fetcher, regardless of whether or not the subtask will initially have
		// shards to subscribe to; fetchers will continuously poll for changes in the shard list, so all subtasks
		// can potentially have new shards to subscribe to later on
		fetcher = new KinesisDataFetcher<>(
			streams, sourceContext, getRuntimeContext(), configProps, deserializer);

		boolean isRestoringFromFailure = (sequenceNumsToRestore != null);
		fetcher.setIsRestoringFromFailure(isRestoringFromFailure);

		// if we are restoring from a checkpoint, we iterate over the restored
		// state and accordingly seed the fetcher with subscribed shards states
		if (isRestoringFromFailure) {
			for (Map.Entry<KinesisStreamShard, SequenceNumber> restored : lastStateSnapshot.entrySet()) {
				fetcher.advanceLastDiscoveredShardOfStream(
					restored.getKey().getStreamName(), restored.getKey().getShard().getShardId());

				if (LOG.isInfoEnabled()) {
					LOG.info("Subtask {} is seeding the fetcher with restored shard {}," +
							" starting state set to the restored sequence number {}",
						getRuntimeContext().getIndexOfThisSubtask(), restored.getKey().toString(), restored.getValue());
				}
				fetcher.registerNewSubscribedShardState(
					new KinesisStreamShardState(restored.getKey(), restored.getValue()));
			}
		}

		// check that we are running before starting the fetcher
		if (!running) {
			return;
		}

		// start the fetcher loop. The fetcher will stop running only when cancel() or
		// close() is called, or an error is thrown by threads created by the fetcher
		fetcher.runFetcher();

		// check that the fetcher has terminated before fully closing
		fetcher.awaitTermination();
		sourceContext.close();
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
	public HashMap<KinesisStreamShard, SequenceNumber> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		if (lastStateSnapshot == null) {
			LOG.debug("snapshotState() requested on not yet opened source; returning null.");
			return null;
		}

		if (fetcher == null) {
			LOG.debug("snapshotState() requested on not yet running source; returning null.");
			return null;
		}

		if (!running) {
			LOG.debug("snapshotState() called on closed source; returning null.");
			return null;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Snapshotting state ...");
		}

		lastStateSnapshot = fetcher.snapshotState();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Snapshotted state, last processed sequence numbers: {}, checkpoint id: {}, timestamp: {}",
				lastStateSnapshot.toString(), checkpointId, checkpointTimestamp);
		}

		return lastStateSnapshot;
	}

	@Override
	public void restoreState(HashMap<KinesisStreamShard, SequenceNumber> restoredState) throws Exception {
		sequenceNumsToRestore = restoredState;
	}
}
