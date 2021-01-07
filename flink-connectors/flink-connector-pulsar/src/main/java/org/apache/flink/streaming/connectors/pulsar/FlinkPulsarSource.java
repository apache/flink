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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.streaming.connectors.pulsar.internal.CachedPulsarClient;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarCommitCallback;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarFetcher;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.internal.SourceSinkUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicRange;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPunctuatedWatermarksAdapter;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.shade.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.internal.metrics.PulsarSourceMetrics.COMMITS_FAILED_METRICS_COUNTER;
import static org.apache.flink.streaming.connectors.pulsar.internal.metrics.PulsarSourceMetrics.COMMITS_SUCCEEDED_METRICS_COUNTER;
import static org.apache.flink.streaming.connectors.pulsar.internal.metrics.PulsarSourceMetrics.PULSAR_SOURCE_METRICS_GROUP;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Pulsar data source.
 *
 * @param <T> The type of records produced by this data source.
 */
@PublicEvolving
public class FlinkPulsarSource<T>
	extends RichParallelSourceFunction<T>
	implements ResultTypeQueryable<T>,
	CheckpointListener,
	CheckpointedFunction {

	private static final Logger log = LoggerFactory.getLogger(FlinkPulsarSource.class);
	/** The maximum number of pending non-committed checkpoints to track, to avoid memory leaks. */
	public static final int MAX_NUM_PENDING_CHECKPOINTS = 100;

	/** Boolean configuration key to disable metrics tracking. **/
	public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

	/** State name of the consumer's partition offset states. */
	private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";

	// ------------------------------------------------------------------------
	//  configuration state, set on the client relevant for all subtasks
	// ------------------------------------------------------------------------

	protected String adminUrl;

	protected ClientConfigurationData clientConfigurationData;

	protected final Map<String, String> caseInsensitiveParams;

	protected final Map<String, Object> readerConf;

	protected volatile PulsarDeserializationSchema<T> deserializer;

	private Map<TopicRange, MessageId> ownedTopicStarts;

	/**
	 * Optional watermark strategy that will be run per pulsar partition, to exploit per-partition
	 * timestamp characteristics. The watermark strategy is kept in serialized form, to deserialize
	 * it into multiple copies.
	 */
	private SerializedValue<WatermarkStrategy<T>> watermarkStrategy;

	/** User configured value for discovery interval, in milliseconds. */
	private final long discoveryIntervalMillis;

	protected final int pollTimeoutMs;

	protected final int commitMaxRetries;

	/** The startup mode for the reader (default is {@link StartupMode#LATEST}). */
	private StartupMode startupMode = StartupMode.LATEST;

	/** Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}. */
	private transient Map<TopicRange, MessageId> specificStartupOffsets;

	/**
	 * The subscription name to be used; only relevant when startup mode is {@link StartupMode#EXTERNAL_SUBSCRIPTION}
	 * If the subscription exists for a partition, we would start reading this partition from the subscription cursor.
	 * At the same time, checkpoint for the job would made progress on the subscription.
	 */
	private String externalSubscriptionName;

	/**
	 * The subscription position to use when subscription does not exist (default is {@link MessageId#latest});
	 * Only relevant when startup mode is {@link StartupMode#EXTERNAL_SUBSCRIPTION}.
	 */
	private MessageId subscriptionPosition = MessageId.latest;

	// TODO: remove this when MessageId is serializable itself.
	// see: https://github.com/apache/pulsar/pull/6064
	private Map<TopicRange, byte[]> specificStartupOffsetsAsBytes;

	protected final Properties properties;

	protected final UUID uuid = UUID.randomUUID();

	// ------------------------------------------------------------------------
	//  runtime state (used individually by each parallel subtask)
	// ------------------------------------------------------------------------

	/** Data for pending but uncommitted offsets. */
	private final LinkedMap pendingOffsetsToCommit = new LinkedMap();

	/** Fetcher implements Pulsar reads. */
	private transient volatile PulsarFetcher<T> pulsarFetcher;

	/** The partition discoverer, used to find new partitions. */
	protected transient volatile PulsarMetadataReader metadataReader;

	/**
	 * The offsets to restore to, if the consumer restores state from a checkpoint.
	 *
	 * <p>This map will be populated by the {@link #initializeState(FunctionInitializationContext)} method.
	 *
	 * <p>Using a sorted map as the ordering is important when using restored state
	 * to seed the partition discoverer.
	 */
	private transient volatile TreeMap<TopicRange, MessageId> restoredState;

	/** Accessor for state in the operator state backend. */

	private transient ListState<Tuple3<TopicRange, MessageId, String>> unionOffsetStates;

	private volatile boolean stateSubEqualexternalSub = false;

	/** Discovery loop, executed in a separate thread. */
	private transient volatile Thread discoveryLoopThread;

	/** Flag indicating whether the consumer is still running. */
	private volatile boolean running = true;

	/**
	 * Flag indicating whether or not metrics should be exposed.
	 * If {@code true}, offset metrics (e.g. current offset, committed offset) and
	 * other metrics will be registered.
	 */
	private final boolean useMetrics;

	/** Counter for successful Pulsar offset commits. */
	private transient Counter successfulCommits;

	/** Counter for failed Pulsar offset commits. */
	private transient Counter failedCommits;

	/** Callback interface that will be invoked upon async pulsar commit completion. */
	private transient PulsarCommitCallback offsetCommitCallback;

	private transient int taskIndex;

	private transient int numParallelTasks;

	public FlinkPulsarSource(
		String adminUrl,
		ClientConfigurationData clientConf,
		PulsarDeserializationSchema<T> deserializer,
		Properties properties) {
		this.adminUrl = checkNotNull(adminUrl);
		this.clientConfigurationData = checkNotNull(clientConf);
		this.deserializer = deserializer;
		this.properties = properties;
		this.caseInsensitiveParams =
			SourceSinkUtils.validateStreamSourceOptions(Maps.fromProperties(properties));
		this.readerConf =
			SourceSinkUtils.getReaderParams(Maps.fromProperties(properties));
		this.discoveryIntervalMillis =
			SourceSinkUtils.getPartitionDiscoveryIntervalInMillis(caseInsensitiveParams);
		this.pollTimeoutMs =
			SourceSinkUtils.getPollTimeoutMs(caseInsensitiveParams);
		this.commitMaxRetries =
			SourceSinkUtils.getCommitMaxRetries(caseInsensitiveParams);
		this.useMetrics =
			SourceSinkUtils.getUseMetrics(caseInsensitiveParams);

		CachedPulsarClient.setCacheSize(SourceSinkUtils.getClientCacheSize(caseInsensitiveParams));

		if (this.clientConfigurationData.getServiceUrl() == null) {
			throw new IllegalArgumentException(
				"ServiceUrl must be supplied in the client configuration");
		}
	}

	public FlinkPulsarSource(
		String serviceUrl,
		String adminUrl,
		PulsarDeserializationSchema<T> deserializer,
		Properties properties) {
		this(
			adminUrl,
			PulsarClientUtils.newClientConf(checkNotNull(serviceUrl), properties),
			deserializer,
			properties);
	}

	// ------------------------------------------------------------------------
	//  Configuration
	// ------------------------------------------------------------------------

	/**
	 * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks
	 * in a punctuated manner. The watermark extractor will run per Pulsar partition,
	 * watermarks will be merged across partitions in the same way as in the Flink runtime,
	 * when streams are merged.
	 *
	 * <p>When a subtask of a FlinkPulsarSource source reads multiple Pulsar partitions,
	 * the streams from the partitions are unioned in a "first come first serve" fashion.
	 * Per-partition characteristics are usually lost that way.
	 * For example, if the timestamps are strictly ascending per Pulsar partition,
	 * they will not be strictly ascending in the resulting Flink DataStream, if the
	 * parallel source subtask reads more that one partition.
	 *
	 * <p>Running timestamp extractors / watermark generators directly inside the Pulsar source,
	 * per Pulsar partition, allows users to let them exploit the per-partition characteristics.
	 *
	 * <p>Note: One can use either an {@link AssignerWithPunctuatedWatermarks} or an
	 * {@link AssignerWithPeriodicWatermarks}, not both at the same time.
	 *
	 * @param assigner The timestamp assigner / watermark generator to use.
	 *
	 * @return The reader object, to allow function chaining.
	 */
	@Deprecated
	public FlinkPulsarSource<T> assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks<T> assigner) {
		checkNotNull(assigner);

		if (this.watermarkStrategy != null) {
			throw new IllegalStateException("Some watermark strategy has already been set.");
		}

		try {
			ClosureCleaner.clean(assigner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
			final WatermarkStrategy<T> wms = new AssignerWithPunctuatedWatermarksAdapter.Strategy<>(
				assigner);

			return assignTimestampsAndWatermarks(wms);
		} catch (Exception e) {
			throw new IllegalArgumentException("The given assigner is not serializable", e);
		}
	}

	/**
	 * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks
	 * in a punctuated manner. The watermark extractor will run per Pulsar partition,
	 * watermarks will be merged across partitions in the same way as in the Flink runtime,
	 * when streams are merged.
	 *
	 * <p>When a subtask of a FlinkPulsarSource source reads multiple Pulsar partitions,
	 * the streams from the partitions are unioned in a "first come first serve" fashion.
	 * Per-partition characteristics are usually lost that way.
	 * For example, if the timestamps are strictly ascending per Pulsar partition,
	 * they will not be strictly ascending in the resulting Flink DataStream,
	 * if the parallel source subtask reads more that one partition.
	 *
	 * <p>Running timestamp extractors / watermark generators directly inside the Pulsar source,
	 * per Pulsar partition, allows users to let them exploit the per-partition characteristics.
	 *
	 * <p>Note: One can use either an {@link AssignerWithPunctuatedWatermarks} or an
	 * {@link AssignerWithPeriodicWatermarks}, not both at the same time.
	 *
	 * @param assigner The timestamp assigner / watermark generator to use.
	 *
	 * @return The reader object, to allow function chaining.
	 */
	@Deprecated
	public FlinkPulsarSource<T> assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks<T> assigner) {
		checkNotNull(assigner);

		if (this.watermarkStrategy != null) {
			throw new IllegalStateException("Some watermark strategy has already been set.");
		}

		try {
			ClosureCleaner.clean(assigner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
			final WatermarkStrategy<T> wms = new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
				assigner);

			return assignTimestampsAndWatermarks(wms);
		} catch (Exception e) {
			throw new IllegalArgumentException("The given assigner is not serializable", e);
		}
	}

	/**
	 * Sets the given {@link WatermarkStrategy} on this consumer. These will be used to assign
	 * timestamps to records and generates watermarks to signal event time progress.
	 *
	 * <p>Running timestamp extractors / watermark generators directly inside the Pulsar source
	 * (which you can do by using this method), per Pulsar partition, allows users to let them
	 * exploit the per-partition characteristics.
	 *
	 * <p>When a subtask of a FlinkPulsarSource reads multiple pulsar partitions,
	 * the streams from the partitions are unioned in a "first come first serve" fashion.
	 * Per-partition characteristics are usually lost that way. For example, if the timestamps are
	 * strictly ascending per Pulsar partition, they will not be strictly ascending in the resulting
	 * Flink DataStream, if the parallel source subtask reads more than one partition.
	 *
	 * <p>Common watermark generation patterns can be found as static methods in the
	 * {@link org.apache.flink.api.common.eventtime.WatermarkStrategy} class.
	 *
	 * @return The consumer object, to allow function chaining.
	 */
	public FlinkPulsarSource<T> assignTimestampsAndWatermarks(
		WatermarkStrategy<T> watermarkStrategy) {
		checkNotNull(watermarkStrategy);

		try {
			ClosureCleaner.clean(
				watermarkStrategy,
				ExecutionConfig.ClosureCleanerLevel.RECURSIVE,
				true);
			this.watermarkStrategy = new SerializedValue<>(watermarkStrategy);
		} catch (Exception e) {
			throw new IllegalArgumentException(
				"The given WatermarkStrategy is not serializable",
				e);
		}

		return this;
	}

	public FlinkPulsarSource<T> setStartFromEarliest() {
		this.startupMode = StartupMode.EARLIEST;
		this.specificStartupOffsets = null;
		return this;
	}

	public FlinkPulsarSource<T> setStartFromLatest() {
		this.startupMode = StartupMode.LATEST;
		this.specificStartupOffsets = null;
		return this;
	}

	public FlinkPulsarSource<T> setStartFromSpecificOffsets(Map<String, MessageId> specificStartupOffsets) {
		checkNotNull(specificStartupOffsets);
		this.specificStartupOffsets = specificStartupOffsets.entrySet()
			.stream()
			.collect(Collectors.toMap(e -> new TopicRange(e.getKey()), Map.Entry::getValue));
		this.startupMode = StartupMode.SPECIFIC_OFFSETS;
		this.specificStartupOffsetsAsBytes = new HashMap<>();
		for (Map.Entry<TopicRange, MessageId> entry : this.specificStartupOffsets.entrySet()) {
			specificStartupOffsetsAsBytes.put(entry.getKey(), entry.getValue().toByteArray());
		}
		return this;
	}

	public FlinkPulsarSource<T> setStartFromSubscription(String externalSubscriptionName) {
		this.startupMode = StartupMode.EXTERNAL_SUBSCRIPTION;
		this.externalSubscriptionName = checkNotNull(externalSubscriptionName);
		return this;
	}

	public FlinkPulsarSource<T> setStartFromSubscription(
		String externalSubscriptionName,
		MessageId subscriptionPosition) {
		this.startupMode = StartupMode.EXTERNAL_SUBSCRIPTION;
		this.externalSubscriptionName = checkNotNull(externalSubscriptionName);
		this.subscriptionPosition = checkNotNull(subscriptionPosition);
		return this;
	}

	// ------------------------------------------------------------------------
	//  Work methods
	// ------------------------------------------------------------------------

	@Override
	public void open(Configuration parameters) throws Exception {
		if (this.deserializer != null) {
			this.deserializer.open(
				RuntimeContextInitializationContextAdapters.deserializationAdapter(
					getRuntimeContext(),
					metricGroup -> metricGroup.addGroup("user")
				)
			);
		}
		this.taskIndex = getRuntimeContext().getIndexOfThisSubtask();
		this.numParallelTasks = getRuntimeContext().getNumberOfParallelSubtasks();

		this.metadataReader = createMetadataReader();

		ownedTopicStarts = new HashMap<>();
		Set<TopicRange> allTopics = metadataReader.discoverTopicChanges();

		if (specificStartupOffsets == null && specificStartupOffsetsAsBytes != null) {
			specificStartupOffsets = new HashMap<>();
			for (Map.Entry<TopicRange, byte[]> entry : specificStartupOffsetsAsBytes.entrySet()) {
				specificStartupOffsets.put(
					entry.getKey(),
					MessageId.fromByteArray(entry.getValue()));
			}
		}
		Map<TopicRange, MessageId> allTopicOffsets =
			offsetForEachTopic(allTopics, startupMode, specificStartupOffsets);

		boolean usingRestoredState =
			(startupMode != StartupMode.EXTERNAL_SUBSCRIPTION) || stateSubEqualexternalSub;

		if (restoredState != null && usingRestoredState) {
			allTopicOffsets.entrySet().stream()
				.filter(e -> !restoredState.containsKey(e.getKey()))
				.forEach(e -> restoredState.put(e.getKey(), e.getValue()));

			restoredState.entrySet().stream()
				.filter(e -> SourceSinkUtils.belongsTo(e.getKey(), numParallelTasks, taskIndex))
				.forEach(e -> ownedTopicStarts.put(e.getKey(), e.getValue()));

			Set<TopicRange> goneTopics = Sets.difference(restoredState.keySet(), allTopics).stream()
				.filter(k -> SourceSinkUtils.belongsTo(k, numParallelTasks, taskIndex))
				.collect(Collectors.toSet());

			for (TopicRange goneTopic : goneTopics) {
				log.warn(goneTopic + " is removed from subscription since " +
					"it no longer matches with topics settings.");
				ownedTopicStarts.remove(goneTopic);
			}

			log.info("Source {} will start reading %d topics in restored state {}",
				taskIndex, ownedTopicStarts.size(), StringUtils.join(ownedTopicStarts.entrySet()));
		} else {
			ownedTopicStarts.putAll(allTopicOffsets.entrySet().stream()
				.filter(e -> SourceSinkUtils.belongsTo(e.getKey(), numParallelTasks, taskIndex))
				.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));

			if (ownedTopicStarts.isEmpty()) {
				log.info("Source {} initially has no topics to read from.", taskIndex);
			} else {
				log.info("Source {} will start reading {} topics from initialized positions: {}",
					taskIndex, ownedTopicStarts.size(), ownedTopicStarts);
			}
		}
	}

	protected String getSubscriptionName() {
		if (startupMode == StartupMode.EXTERNAL_SUBSCRIPTION) {
			checkNotNull(externalSubscriptionName);
			return externalSubscriptionName;
		} else {
			return "flink-pulsar-" + uuid.toString();
		}
	}

	protected PulsarMetadataReader createMetadataReader() throws PulsarClientException {
		return new PulsarMetadataReader(
			adminUrl,
			clientConfigurationData,
			getSubscriptionName(),
			caseInsensitiveParams,
			taskIndex,
			numParallelTasks,
			startupMode == StartupMode.EXTERNAL_SUBSCRIPTION);
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		if (ownedTopicStarts == null) {
			throw new Exception("The partitions were not set for the source");
		}

		this.successfulCommits =
			this.getRuntimeContext().getMetricGroup().counter(COMMITS_SUCCEEDED_METRICS_COUNTER);
		this.failedCommits =
			this.getRuntimeContext().getMetricGroup().counter(COMMITS_FAILED_METRICS_COUNTER);

		this.offsetCommitCallback = new PulsarCommitCallback() {
			@Override
			public void onSuccess() {
				successfulCommits.inc();
			}

			@Override
			public void onException(Throwable cause) {
				log.warn("source {} failed commit by {}", taskIndex, cause.toString());
				failedCommits.inc();
			}
		};

		if (ownedTopicStarts.isEmpty()) {
			ctx.markAsTemporarilyIdle();
		}

		log.info(
			"Source {} creating fetcher with offsets {}",
			taskIndex,
			StringUtils.join(ownedTopicStarts.entrySet()));

		// from this point forward:
		//   - 'snapshotState' will draw offsets from the fetcher,
		//     instead of being built from `subscribedPartitionsToStartOffsets`
		//   - 'notifyCheckpointComplete' will start to do work (i.e. commit offsets to
		//     Pulsar through the fetcher, if configured to do so)

		StreamingRuntimeContext streamingRuntime = (StreamingRuntimeContext) getRuntimeContext();

		this.pulsarFetcher = createFetcher(
			ctx,
			ownedTopicStarts,
			watermarkStrategy,
			streamingRuntime.getProcessingTimeService(),
			streamingRuntime.getExecutionConfig().getAutoWatermarkInterval(),
			getRuntimeContext().getUserCodeClassLoader(),
			streamingRuntime,
			useMetrics);

		if (!running) {
			return;
		}

		if (discoveryIntervalMillis < 0) {
			pulsarFetcher.runFetchLoop();
		} else {
			runWithTopicsDiscovery();
		}
	}

	protected PulsarFetcher<T> createFetcher(
		SourceContext<T> sourceContext,
		Map<TopicRange, MessageId> seedTopicsWithInitialOffsets,
		SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
		ProcessingTimeService processingTimeProvider,
		long autoWatermarkInterval,
		ClassLoader userCodeClassLoader,
		StreamingRuntimeContext streamingRuntime,
		boolean useMetrics) throws Exception {

		//readerConf.putIfAbsent(PulsarOptions.SUBSCRIPTION_ROLE_OPTION_KEY, getSubscriptionName());

		return new PulsarFetcher<>(
			sourceContext,
			seedTopicsWithInitialOffsets,
			watermarkStrategy,
			processingTimeProvider,
			autoWatermarkInterval,
			userCodeClassLoader,
			streamingRuntime,
			clientConfigurationData,
			readerConf,
			pollTimeoutMs,
			commitMaxRetries,
			deserializer,
			metadataReader,
			streamingRuntime.getMetricGroup().addGroup(PULSAR_SOURCE_METRICS_GROUP),
			useMetrics);
	}

	public void joinDiscoveryLoopThread() throws InterruptedException {
		if (discoveryLoopThread != null) {
			discoveryLoopThread.join();
		}
	}

	public void runWithTopicsDiscovery() throws Exception {
		AtomicReference<Exception> discoveryLoopErrorRef = new AtomicReference<>();
		createAndStartDiscoveryLoop(discoveryLoopErrorRef);

		pulsarFetcher.runFetchLoop();

		joinDiscoveryLoopThread();

		Exception discoveryLoopError = discoveryLoopErrorRef.get();
		if (discoveryLoopError != null) {
			throw new RuntimeException(discoveryLoopError);
		}
	}

	private void createAndStartDiscoveryLoop(AtomicReference<Exception> discoveryLoopErrorRef) {
		discoveryLoopThread = new Thread(
			() -> {
				try {
					while (running) {
						Set<TopicRange> added = metadataReader.discoverTopicChanges();

						if (running && !added.isEmpty()) {
							pulsarFetcher.addDiscoveredTopics(added);
						}

						if (running && discoveryIntervalMillis != -1) {
							Thread.sleep(discoveryIntervalMillis);
						}
					}
				} catch (PulsarMetadataReader.ClosedException e) {
					// break out while and do nothing
				} catch (InterruptedException e) {
					// break out while and do nothing
				} catch (Exception e) {
					discoveryLoopErrorRef.set(e);
				} finally {
					if (running) {
						// calling cancel will also let the fetcher loop escape
						// (if not running, cancel() was already called)
						cancel();
					}
				}
			}, "Pulsar topic discovery for source " + taskIndex);
		discoveryLoopThread.start();
	}

	@Override
	public void close() throws Exception {
		cancel();

		joinDiscoveryLoopThread();

		Exception exception = null;

		if (metadataReader != null) {
			try {
				metadataReader.close();
			} catch (Exception e) {
				exception = e;
			}
		}

		try {
			super.close();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			throw exception;
		}
	}

	@Override
	public void cancel() {
		running = false;

		if (discoveryLoopThread != null) {
			discoveryLoopThread.interrupt();
		}

		if (pulsarFetcher != null) {
			try {
				pulsarFetcher.cancel();
			} catch (Exception e) {
				log.error(
					"Failed to cancel the Pulsar Fetcher {}",
					ExceptionUtils.stringifyException(e));
				throw new RuntimeException(e);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  ResultTypeQueryable methods
	// ------------------------------------------------------------------------

	@Override
	public TypeInformation<T> getProducedType() {
		return deserializer.getProducedType();
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and restore
	// ------------------------------------------------------------------------

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		OperatorStateStore stateStore = context.getOperatorStateStore();

		unionOffsetStates = stateStore.getUnionListState(
			new ListStateDescriptor<>(
				OFFSETS_STATE_NAME,
				TypeInformation.of(new TypeHint<Tuple3<TopicRange, MessageId, String>>() {
				})));

		if (context.isRestored()) {
			restoredState = new TreeMap<>();
			unionOffsetStates.get().forEach(e -> restoredState.put(e.f0, e.f1));
			for (Tuple3<TopicRange, MessageId, String> e : unionOffsetStates.get()) {
				if (e.f2 != null && e.f2.equals(externalSubscriptionName)) {
					stateSubEqualexternalSub = true;
					log.info("Source restored state with subscriptionName {}", e.f2);
				}
				break;
			}

			log.info(
				"Source subtask {} restored state {}",
				taskIndex,
				StringUtils.join(restoredState.entrySet()));
		} else {
			log.info("Source subtask {} has no restore state", taskIndex);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (!running) {
			log.debug("snapshotState() called on closed source");
		} else {
			unionOffsetStates.clear();

			PulsarFetcher fetcher = this.pulsarFetcher;

			if (fetcher == null) {
				// the fetcher has not yet been initialized, which means we need to return the
				// originally restored offsets or the assigned partitions
				for (Map.Entry<TopicRange, MessageId> entry : ownedTopicStarts.entrySet()) {
					unionOffsetStates.add(Tuple3.of(
						entry.getKey(),
						entry.getValue(),
						getSubscriptionName()));
				}
				pendingOffsetsToCommit.put(context.getCheckpointId(), restoredState);
			} else {
				Map<TopicRange, MessageId> currentOffsets = fetcher.snapshotCurrentState();
				pendingOffsetsToCommit.put(context.getCheckpointId(), currentOffsets);
				for (Map.Entry<TopicRange, MessageId> entry : currentOffsets.entrySet()) {
					unionOffsetStates.add(Tuple3.of(
						entry.getKey(),
						entry.getValue(),
						getSubscriptionName()));
				}
				while (pendingOffsetsToCommit.size() > MAX_NUM_PENDING_CHECKPOINTS) {
					pendingOffsetsToCommit.remove(0);
				}
			}
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (!running) {
			log.info("notifyCheckpointComplete() called on closed source");
			return;
		}

		PulsarFetcher<T> fetcher = this.pulsarFetcher;

		if (fetcher == null) {
			log.info("notifyCheckpointComplete() called on uninitialized source");
			return;
		}

		log.debug("Source {} received confirmation for unknown checkpoint id {}",
			taskIndex, checkpointId);

		try {
			int posInMap = pendingOffsetsToCommit.indexOf(checkpointId);
			if (posInMap == -1) {
				log.warn("Source {} received confirmation for unknown checkpoint id {}",
					taskIndex, checkpointId);
				return;
			}

			Map<TopicRange, MessageId> offset = (Map<TopicRange, MessageId>) pendingOffsetsToCommit.remove(
				posInMap);

			// remove older checkpoints in map
			for (int i = 0; i < posInMap; i++) {
				pendingOffsetsToCommit.remove(0);
			}

			if (offset == null || offset.size() == 0) {
				log.debug("Source {} has empty checkpoint state", taskIndex);
				return;
			}
			fetcher.commitOffsetToPulsar(offset, offsetCommitCallback);
		} catch (Exception e) {
			if (running) {
				throw e;
			}
		}
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) throws Exception {
		log.error("checkpoint aborted, checkpointId: {}", checkpointId);
	}

	public Map<TopicRange, MessageId> offsetForEachTopic(
		Set<TopicRange> topics,
		StartupMode mode,
		Map<TopicRange, MessageId> specificStartupOffsets) {

		switch (mode) {
			case LATEST:
				return topics.stream()
					.collect(Collectors.toMap(k -> k, k -> MessageId.latest));
			case EARLIEST:
				return topics.stream()
					.collect(Collectors.toMap(k -> k, k -> MessageId.earliest));
			case SPECIFIC_OFFSETS:
				checkArgument(
					topics.containsAll(specificStartupOffsets.keySet()),
					String.format(
						"Topics designated in startingOffsets should appear in %s, topics:" +
							"%s, topics in offsets: %s",
						StringUtils.join(PulsarOptions.TOPIC_OPTION_KEYS),
						StringUtils.join(topics.toArray()),
						StringUtils.join(specificStartupOffsets.entrySet().toArray())));

				Map<TopicRange, MessageId> specificOffsets = new HashMap<>();
				for (TopicRange topic : topics) {
					if (specificStartupOffsets.containsKey(topic)) {
						specificOffsets.put(topic, specificStartupOffsets.get(topic));
					} else {
						specificOffsets.put(topic, MessageId.latest);
					}
				}
				return specificOffsets;
			case EXTERNAL_SUBSCRIPTION:
				Map<TopicRange, MessageId> offsetsFromSubs = new HashMap<>();
				for (TopicRange topic : topics) {
					offsetsFromSubs.put(topic, metadataReader.getPositionFromSubscription(
						topic.getTopic(),
						subscriptionPosition));
				}
				return offsetsFromSubs;
		}
		return null;
	}

	public LinkedMap getPendingOffsetsToCommit() {
		return pendingOffsetsToCommit;
	}

	public Map<TopicRange, MessageId> getOwnedTopicStarts() {
		return ownedTopicStarts;
	}
}
