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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internal.TransactionalIdsGenerator;
import org.apache.flink.streaming.connectors.kafka.internal.metrics.KafkaMetricMuttableWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaDelegatePartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.NetUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Flink Sink to produce data into a Kafka topic. This producer is compatible with Kafka 0.11.x. By default producer
 * will use {@link Semantic#AT_LEAST_ONCE} semantic. Before using {@link Semantic#EXACTLY_ONCE} please refer to Flink's
 * Kafka connector documentation.
 */
@PublicEvolving
public class FlinkKafkaProducer011<IN>
		extends TwoPhaseCommitSinkFunction<IN, FlinkKafkaProducer011.KafkaTransactionState, FlinkKafkaProducer011.KafkaTransactionContext> {

	/**
	 *  Semantics that can be chosen.
	 *  <li>{@link #EXACTLY_ONCE}</li>
	 *  <li>{@link #AT_LEAST_ONCE}</li>
	 *  <li>{@link #NONE}</li>
	 */
	public enum Semantic {

		/**
		 * Semantic.EXACTLY_ONCE the Flink producer will write all messages in a Kafka transaction that will be
		 * committed to the Kafka on a checkpoint.
		 *
		 * <p>In this mode {@link FlinkKafkaProducer011} sets up a pool of {@link FlinkKafkaProducer}. Between each
		 * checkpoint there is created new Kafka transaction, which is being committed on
		 * {@link FlinkKafkaProducer011#notifyCheckpointComplete(long)}. If checkpoint complete notifications are
		 * running late, {@link FlinkKafkaProducer011} can run out of {@link FlinkKafkaProducer}s in the pool. In that
		 * case any subsequent {@link FlinkKafkaProducer011#snapshotState(FunctionSnapshotContext)} requests will fail
		 * and {@link FlinkKafkaProducer011} will keep using the {@link FlinkKafkaProducer} from previous checkpoint.
		 * To decrease chances of failing checkpoints there are three options:
		 * <li>decrease number of max concurrent checkpoints</li>
		 * <li>make checkpoints more reliable (so that they complete faster)</li>
		 * <li>increase delay between checkpoints</li>
		 * <li>increase size of {@link FlinkKafkaProducer}s pool</li>
		 */
		EXACTLY_ONCE,

		/**
		 * Semantic.AT_LEAST_ONCE the Flink producer will wait for all outstanding messages in the Kafka buffers
		 * to be acknowledged by the Kafka producer on a checkpoint.
		 */
		AT_LEAST_ONCE,

		/**
		 * Semantic.NONE means that nothing will be guaranteed. Messages can be lost and/or duplicated in case
		 * of failure.
		 */
		NONE
	}

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaProducer011.class);

	private static final long serialVersionUID = 1L;

	/**
	 * This coefficient determines what is the safe scale down factor.
	 *
	 * <p>If the Flink application previously failed before first checkpoint completed or we are starting new batch
	 * of {@link FlinkKafkaProducer011} from scratch without clean shutdown of the previous one,
	 * {@link FlinkKafkaProducer011} doesn't know what was the set of previously used Kafka's transactionalId's. In
	 * that case, it will try to play safe and abort all of the possible transactionalIds from the range of:
	 * {@code [0, getNumberOfParallelSubtasks() * kafkaProducersPoolSize * SAFE_SCALE_DOWN_FACTOR) }
	 *
	 * <p>The range of available to use transactional ids is:
	 * {@code [0, getNumberOfParallelSubtasks() * kafkaProducersPoolSize) }
	 *
	 * <p>This means that if we decrease {@code getNumberOfParallelSubtasks()} by a factor larger then
	 * {@code SAFE_SCALE_DOWN_FACTOR} we can have a left some lingering transaction.
	 */
	public static final int SAFE_SCALE_DOWN_FACTOR = 5;

	/**
	 * Default number of KafkaProducers in the pool. See {@link Semantic#EXACTLY_ONCE}.
	 */
	public static final int DEFAULT_KAFKA_PRODUCERS_POOL_SIZE = 5;

	/**
	 * Default value for kafka transaction timeout.
	 */
	public static final Time DEFAULT_KAFKA_TRANSACTION_TIMEOUT = Time.hours(1);

	/**
	 * Configuration key for disabling the metrics reporting.
	 */
	public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

	/**
	 * Descriptor of the transacionalIds list.
	 */
	private static final ListStateDescriptor<NextTransactionalIdHint> NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR =
		new ListStateDescriptor<>("next-transactional-id-hint", TypeInformation.of(NextTransactionalIdHint.class));

	/**
	 * State for nextTransactionalIdHint.
	 */
	private transient ListState<NextTransactionalIdHint> nextTransactionalIdHintState;

	/**
	 * Generator for Transactional IDs.
	 */
	private transient TransactionalIdsGenerator transactionalIdsGenerator;

	/**
	 * Hint for picking next transactional id.
	 */
	private transient NextTransactionalIdHint nextTransactionalIdHint;

	/**
	 * User defined properties for the Producer.
	 */
	private final Properties producerConfig;

	/**
	 * The name of the default topic this producer is writing data to.
	 */
	private final String defaultTopicId;

	/**
	 * (Serializable) SerializationSchema for turning objects used with Flink into.
	 * byte[] for Kafka.
	 */
	private final KeyedSerializationSchema<IN> schema;

	/**
	 * User-provided partitioner for assigning an object to a Kafka partition for each topic.
	 */
	private final FlinkKafkaPartitioner<IN> flinkKafkaPartitioner;

	/**
	 * Partitions of each topic.
	 */
	private final Map<String, int[]> topicPartitionsMap;

	/**
	 * Max number of producers in the pool. If all producers are in use, snapshoting state will throw an exception.
	 */
	private final int kafkaProducersPoolSize;

	/**
	 * Pool of available transactional ids.
	 */
	private final BlockingDeque<String> availableTransactionalIds = new LinkedBlockingDeque<>();

	/**
	 * Flag controlling whether we are writing the Flink record's timestamp into Kafka.
	 */
	private boolean writeTimestampToKafka = false;

	/**
	 * Flag indicating whether to accept failures (and log them), or to fail on failures.
	 */
	private boolean logFailuresOnly;

	/**
	 * Semantic chosen for this instance.
	 */
	private Semantic semantic;

	// -------------------------------- Runtime fields ------------------------------------------

	/** The callback than handles error propagation or logging callbacks. */
	@Nullable
	private transient Callback callback;

	/** Errors encountered in the async producer are stored here. */
	@Nullable
	private transient volatile Exception asyncException;

	/** Number of unacknowledged records. */
	private final AtomicLong pendingRecords = new AtomicLong();

	/** Cache of metrics to replace already registered metrics instead of overwriting existing ones. */
	private final Map<String, KafkaMetricMuttableWrapper> previouslyCreatedMetrics = new HashMap<>();

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * @param brokerList
	 *			Comma separated addresses of the brokers
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined (keyless) serialization schema.
	 */
	public FlinkKafkaProducer011(String brokerList, String topicId, SerializationSchema<IN> serializationSchema) {
		this(
			topicId,
			new KeyedSerializationSchemaWrapper<>(serializationSchema),
			getPropertiesFromBrokerList(brokerList),
			Optional.of(new FlinkFixedPartitioner<IN>()));
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single Kafka
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * Kafka partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkKafkaProducer011(String, SerializationSchema, Properties, Optional)} instead.
	 *
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined key-less serialization schema.
	 * @param producerConfig
	 * 			Properties with the producer configuration.
	 */
	public FlinkKafkaProducer011(String topicId, SerializationSchema<IN> serializationSchema, Properties producerConfig) {
		this(
			topicId,
			new KeyedSerializationSchemaWrapper<>(serializationSchema),
			producerConfig,
			Optional.of(new FlinkFixedPartitioner<IN>()));
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces its input to
	 * the topic. It accepts a key-less {@link SerializationSchema} and possibly a custom {@link FlinkKafkaPartitioner}.
	 *
	 * <p>Since a key-less {@link SerializationSchema} is used, all records sent to Kafka will not have an
	 * attached key. Therefore, if a partitioner is also not provided, records will be distributed to Kafka
	 * partitions in a round-robin fashion.
	 *
	 * @param topicId The topic to write data to
	 * @param serializationSchema A key-less serializable serialization schema for turning user objects into a kafka-consumable byte[]
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions.
	 *                          If a partitioner is not provided, records will be distributed to Kafka partitions
	 *                          in a round-robin fashion.
	 */
	public FlinkKafkaProducer011(
			String topicId,
			SerializationSchema<IN> serializationSchema,
			Properties producerConfig,
			Optional<FlinkKafkaPartitioner<IN>> customPartitioner) {

		this(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), producerConfig, customPartitioner);
	}

	// ------------------- Key/Value serialization schema constructors ----------------------

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single Kafka
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * Kafka partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkKafkaProducer011(String, KeyedSerializationSchema, Properties, Optional)} instead.
	 *
	 * @param brokerList
	 *			Comma separated addresses of the brokers
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined serialization schema supporting key/value messages
	 */
	public FlinkKafkaProducer011(String brokerList, String topicId, KeyedSerializationSchema<IN> serializationSchema) {
		this(
			topicId,
			serializationSchema,
			getPropertiesFromBrokerList(brokerList),
			Optional.of(new FlinkFixedPartitioner<IN>()));
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single Kafka
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * Kafka partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkKafkaProducer011(String, KeyedSerializationSchema, Properties, Optional)} instead.
	 *
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined serialization schema supporting key/value messages
	 * @param producerConfig
	 * 			Properties with the producer configuration.
	 */
	public FlinkKafkaProducer011(String topicId, KeyedSerializationSchema<IN> serializationSchema, Properties producerConfig) {
		this(
			topicId,
			serializationSchema,
			producerConfig,
			Optional.of(new FlinkFixedPartitioner<IN>()));
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single Kafka
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * Kafka partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkKafkaProducer011(String, KeyedSerializationSchema, Properties, Optional, Semantic, int)} instead.
	 *
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined serialization schema supporting key/value messages
	 * @param producerConfig
	 * 			Properties with the producer configuration.
	 * @param semantic
	 * 			Defines semantic that will be used by this producer (see {@link Semantic}).
	 */
	public FlinkKafkaProducer011(
			String topicId,
			KeyedSerializationSchema<IN> serializationSchema,
			Properties producerConfig,
			Semantic semantic) {
		this(topicId,
			serializationSchema,
			producerConfig,
			Optional.of(new FlinkFixedPartitioner<IN>()),
			semantic,
			DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);
	}


	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces its input to
	 * the topic. It accepts a keyed {@link KeyedSerializationSchema} and possibly a custom {@link FlinkKafkaPartitioner}.
	 *
	 * <p>If a partitioner is not provided, written records will be partitioned by the attached key of each
	 * record (as determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If written records do not
	 * have a key (i.e., {@link KeyedSerializationSchema#serializeKey(Object)} returns {@code null}), they
	 * will be distributed to Kafka partitions in a round-robin fashion.
	 *
	 * @param defaultTopicId The default topic to write data to
	 * @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions.
	 *                          If a partitioner is not provided, records will be partitioned by the key of each record
	 *                          (determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If the keys
	 *                          are {@code null}, then records will be distributed to Kafka partitions in a
	 *                          round-robin fashion.
	 */
	public FlinkKafkaProducer011(
			String defaultTopicId,
			KeyedSerializationSchema<IN> serializationSchema,
			Properties producerConfig,
			Optional<FlinkKafkaPartitioner<IN>> customPartitioner) {
		this(
			defaultTopicId,
			serializationSchema,
			producerConfig,
			customPartitioner,
			Semantic.AT_LEAST_ONCE,
			DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces its input to
	 * the topic. It accepts a keyed {@link KeyedSerializationSchema} and possibly a custom {@link FlinkKafkaPartitioner}.
	 *
	 * <p>If a partitioner is not provided, written records will be partitioned by the attached key of each
	 * record (as determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If written records do not
	 * have a key (i.e., {@link KeyedSerializationSchema#serializeKey(Object)} returns {@code null}), they
	 * will be distributed to Kafka partitions in a round-robin fashion.
	 *
	 * @param defaultTopicId The default topic to write data to
	 * @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions.
	 *                          If a partitioner is not provided, records will be partitioned by the key of each record
	 *                          (determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If the keys
	 *                          are {@code null}, then records will be distributed to Kafka partitions in a
	 *                          round-robin fashion.
	 * @param semantic Defines semantic that will be used by this producer (see {@link Semantic}).
	 * @param kafkaProducersPoolSize Overwrite default KafkaProducers pool size (see {@link Semantic#EXACTLY_ONCE}).
	 */
	public FlinkKafkaProducer011(
			String defaultTopicId,
			KeyedSerializationSchema<IN> serializationSchema,
			Properties producerConfig,
			Optional<FlinkKafkaPartitioner<IN>> customPartitioner,
			Semantic semantic,
			int kafkaProducersPoolSize) {
		super(new TransactionStateSerializer(), new ContextStateSerializer());

		this.defaultTopicId = checkNotNull(defaultTopicId, "defaultTopicId is null");
		this.schema = checkNotNull(serializationSchema, "serializationSchema is null");
		this.producerConfig = checkNotNull(producerConfig, "producerConfig is null");
		this.flinkKafkaPartitioner = checkNotNull(customPartitioner, "customPartitioner is null").orElse(null);
		this.semantic = checkNotNull(semantic, "semantic is null");
		this.kafkaProducersPoolSize = kafkaProducersPoolSize;
		checkState(kafkaProducersPoolSize > 0, "kafkaProducersPoolSize must be non empty");

		ClosureCleaner.clean(this.flinkKafkaPartitioner, true);
		ClosureCleaner.ensureSerializable(serializationSchema);

		// set the producer configuration properties for kafka record key value serializers.
		if (!producerConfig.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
			this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
		}

		if (!producerConfig.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
			this.producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
		}

		// eagerly ensure that bootstrap servers are set.
		if (!this.producerConfig.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
			throw new IllegalArgumentException(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + " must be supplied in the producer config properties.");
		}

		if (!producerConfig.containsKey(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG)) {
			long timeout = DEFAULT_KAFKA_TRANSACTION_TIMEOUT.toMilliseconds();
			checkState(timeout < Integer.MAX_VALUE && timeout > 0, "timeout does not fit into 32 bit integer");
			this.producerConfig.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, (int) timeout);
			LOG.warn("Property [{}] not specified. Setting it to {}", ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, DEFAULT_KAFKA_TRANSACTION_TIMEOUT);
		}

		// Enable transactionTimeoutWarnings to avoid silent data loss
		// See KAFKA-6119 (affects versions 0.11.0.0 and 0.11.0.1):
		// The KafkaProducer may not throw an exception if the transaction failed to commit
		if (semantic == Semantic.EXACTLY_ONCE) {
			final Object object = this.producerConfig.get(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
			final long transactionTimeout;
			if (object instanceof String && StringUtils.isNumeric((String) object)) {
				transactionTimeout = Long.parseLong((String) object);
			} else if (object instanceof Number) {
				transactionTimeout = ((Number) object).longValue();
			} else {
				throw new IllegalArgumentException(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG
					+ " must be numeric, was " + object);
			}
			super.setTransactionTimeout(transactionTimeout);
			super.enableTransactionTimeoutWarnings(0.8);
		}

		this.topicPartitionsMap = new HashMap<>();
	}

	// ---------------------------------- Properties --------------------------

	/**
	 * If set to true, Flink will write the (event time) timestamp attached to each record into Kafka.
	 * Timestamps must be positive for Kafka to accept them.
	 *
	 * @param writeTimestampToKafka Flag indicating if Flink's internal timestamps are written to Kafka.
	 */
	public void setWriteTimestampToKafka(boolean writeTimestampToKafka) {
		this.writeTimestampToKafka = writeTimestampToKafka;
	}

	/**
	 * Defines whether the producer should fail on errors, or only log them.
	 * If this is set to true, then exceptions will be only logged, if set to false,
	 * exceptions will be eventually thrown and cause the streaming program to
	 * fail (and enter recovery).
	 *
	 * @param logFailuresOnly The flag to indicate logging-only on exceptions.
	 */
	public void setLogFailuresOnly(boolean logFailuresOnly) {
		this.logFailuresOnly = logFailuresOnly;
	}

	/**
	 * Disables the propagation of exceptions thrown when committing presumably timed out Kafka
	 * transactions during recovery of the job. If a Kafka transaction is timed out, a commit will
	 * never be successful. Hence, use this feature to avoid recovery loops of the Job. Exceptions
	 * will still be logged to inform the user that data loss might have occurred.
	 *
	 * <p>Note that we use {@link System#currentTimeMillis()} to track the age of a transaction.
	 * Moreover, only exceptions thrown during the recovery are caught, i.e., the producer will
	 * attempt at least one commit of the transaction before giving up.</p>
	 */
	@Override
	public FlinkKafkaProducer011<IN> ignoreFailuresAfterTransactionTimeout() {
		super.ignoreFailuresAfterTransactionTimeout();
		return this;
	}

	// ----------------------------------- Utilities --------------------------

	/**
	 * Initializes the connection to Kafka.
	 */
	@Override
	public void open(Configuration configuration) throws Exception {
		if (logFailuresOnly) {
			callback = new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null) {
						LOG.error("Error while sending record to Kafka: " + e.getMessage(), e);
					}
					acknowledgeMessage();
				}
			};
		}
		else {
			callback = new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception != null && asyncException == null) {
						asyncException = exception;
					}
					acknowledgeMessage();
				}
			};
		}

		super.open(configuration);
	}

	@Override
	public void invoke(KafkaTransactionState transaction, IN next, Context context) throws FlinkKafka011Exception {
		checkErroneous();

		byte[] serializedKey = schema.serializeKey(next);
		byte[] serializedValue = schema.serializeValue(next);
		String targetTopic = schema.getTargetTopic(next);
		if (targetTopic == null) {
			targetTopic = defaultTopicId;
		}

		Long timestamp = null;
		if (this.writeTimestampToKafka) {
			timestamp = context.timestamp();
		}

		ProducerRecord<byte[], byte[]> record;
		int[] partitions = topicPartitionsMap.get(targetTopic);
		if (null == partitions) {
			partitions = getPartitionsByTopic(targetTopic, transaction.producer);
			topicPartitionsMap.put(targetTopic, partitions);
		}
		if (flinkKafkaPartitioner != null) {
			record = new ProducerRecord<>(
				targetTopic,
				flinkKafkaPartitioner.partition(next, serializedKey, serializedValue, targetTopic, partitions),
				timestamp,
				serializedKey,
				serializedValue);
		} else {
			record = new ProducerRecord<>(targetTopic, null, timestamp, serializedKey, serializedValue);
		}
		pendingRecords.incrementAndGet();
		transaction.producer.send(record, callback);
	}

	@Override
	public void close() throws FlinkKafka011Exception {
		final KafkaTransactionState currentTransaction = currentTransaction();
		if (currentTransaction != null) {
			// to avoid exceptions on aborting transactions with some pending records
			flush(currentTransaction);
		}
		try {
			super.close();
		}
		catch (Exception e) {
			asyncException = ExceptionUtils.firstOrSuppressed(e, asyncException);
		}
		// make sure we propagate pending errors
		checkErroneous();
	}

	// ------------------- Logic for handling checkpoint flushing -------------------------- //

	@Override
	protected KafkaTransactionState beginTransaction() throws FlinkKafka011Exception {
		switch (semantic) {
			case EXACTLY_ONCE:
				FlinkKafkaProducer<byte[], byte[]> producer = createTransactionalProducer();
				producer.beginTransaction();
				return new KafkaTransactionState(producer.getTransactionalId(), producer);
			case AT_LEAST_ONCE:
			case NONE:
				// Do not create new producer on each beginTransaction() if it is not necessary
				final KafkaTransactionState currentTransaction = currentTransaction();
				if (currentTransaction != null && currentTransaction.producer != null) {
					return new KafkaTransactionState(currentTransaction.producer);
				}
				return new KafkaTransactionState(initProducer(true));
			default:
				throw new UnsupportedOperationException("Not implemented semantic");
		}
	}

	@Override
	protected void preCommit(KafkaTransactionState transaction) throws FlinkKafka011Exception {
		switch (semantic) {
			case EXACTLY_ONCE:
			case AT_LEAST_ONCE:
				flush(transaction);
				break;
			case NONE:
				break;
			default:
				throw new UnsupportedOperationException("Not implemented semantic");
		}
		checkErroneous();
	}

	@Override
	protected void commit(KafkaTransactionState transaction) {
		switch (semantic) {
			case EXACTLY_ONCE:
				transaction.producer.commitTransaction();
				recycleTransactionalProducer(transaction.producer);
				break;
			case AT_LEAST_ONCE:
			case NONE:
				break;
			default:
				throw new UnsupportedOperationException("Not implemented semantic");
		}
	}

	@Override
	protected void recoverAndCommit(KafkaTransactionState transaction) {
		switch (semantic) {
			case EXACTLY_ONCE:
				try (FlinkKafkaProducer<byte[], byte[]> producer =
						initTransactionalProducer(transaction.transactionalId, false)) {
					producer.resumeTransaction(transaction.producerId, transaction.epoch);
					producer.commitTransaction();
				}
				catch (InvalidTxnStateException | ProducerFencedException ex) {
					// That means we have committed this transaction before.
					LOG.warn("Encountered error {} while recovering transaction {}. " +
						"Presumably this transaction has been already committed before",
						ex,
						transaction);
				}
				break;
			case AT_LEAST_ONCE:
			case NONE:
				break;
			default:
				throw new UnsupportedOperationException("Not implemented semantic");
		}
	}

	@Override
	protected void abort(KafkaTransactionState transaction) {
		switch (semantic) {
			case EXACTLY_ONCE:
				transaction.producer.abortTransaction();
				recycleTransactionalProducer(transaction.producer);
				break;
			case AT_LEAST_ONCE:
			case NONE:
				break;
			default:
				throw new UnsupportedOperationException("Not implemented semantic");
		}
	}

	@Override
	protected void recoverAndAbort(KafkaTransactionState transaction) {
		switch (semantic) {
			case EXACTLY_ONCE:
				try (FlinkKafkaProducer<byte[], byte[]> producer =
						initTransactionalProducer(transaction.transactionalId, false)) {
					producer.initTransactions();
				}
				break;
			case AT_LEAST_ONCE:
			case NONE:
				break;
			default:
				throw new UnsupportedOperationException("Not implemented semantic");
		}
	}

	private void acknowledgeMessage() {
		pendingRecords.decrementAndGet();
	}

	/**
	 * Flush pending records.
	 * @param transaction
	 */
	private void flush(KafkaTransactionState transaction) throws FlinkKafka011Exception {
		if (transaction.producer != null) {
			transaction.producer.flush();
		}
		long pendingRecordsCount = pendingRecords.get();
		if (pendingRecordsCount != 0) {
			throw new IllegalStateException("Pending record count must be zero at this point: " + pendingRecordsCount);
		}

		// if the flushed requests has errors, we should propagate it also and fail the checkpoint
		checkErroneous();
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		super.snapshotState(context);

		nextTransactionalIdHintState.clear();
		// To avoid duplication only first subtask keeps track of next transactional id hint. Otherwise all of the
		// subtasks would write exactly same information.
		if (getRuntimeContext().getIndexOfThisSubtask() == 0 && semantic == Semantic.EXACTLY_ONCE) {
			checkState(nextTransactionalIdHint != null, "nextTransactionalIdHint must be set for EXACTLY_ONCE");
			long nextFreeTransactionalId = nextTransactionalIdHint.nextFreeTransactionalId;

			// If we scaled up, some (unknown) subtask must have created new transactional ids from scratch. In that
			// case we adjust nextFreeTransactionalId by the range of transactionalIds that could be used for this
			// scaling up.
			if (getRuntimeContext().getNumberOfParallelSubtasks() > nextTransactionalIdHint.lastParallelism) {
				nextFreeTransactionalId += getRuntimeContext().getNumberOfParallelSubtasks() * kafkaProducersPoolSize;
			}

			nextTransactionalIdHintState.add(new NextTransactionalIdHint(
				getRuntimeContext().getNumberOfParallelSubtasks(),
				nextFreeTransactionalId));
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		if (semantic != Semantic.NONE && !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
			LOG.warn("Using {} semantic, but checkpointing is not enabled. Switching to {} semantic.", semantic, Semantic.NONE);
			semantic = Semantic.NONE;
		}

		nextTransactionalIdHintState = context.getOperatorStateStore().getUnionListState(
			NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR);
		transactionalIdsGenerator = new TransactionalIdsGenerator(
			getRuntimeContext().getTaskName(),
			getRuntimeContext().getIndexOfThisSubtask(),
			getRuntimeContext().getNumberOfParallelSubtasks(),
			kafkaProducersPoolSize,
			SAFE_SCALE_DOWN_FACTOR);

		if (semantic != Semantic.EXACTLY_ONCE) {
			nextTransactionalIdHint = null;
		} else {
			ArrayList<NextTransactionalIdHint> transactionalIdHints = Lists.newArrayList(nextTransactionalIdHintState.get());
			if (transactionalIdHints.size() > 1) {
				throw new IllegalStateException(
					"There should be at most one next transactional id hint written by the first subtask");
			} else if (transactionalIdHints.size() == 0) {
				nextTransactionalIdHint = new NextTransactionalIdHint(0, 0);

				// this means that this is either:
				// (1) the first execution of this application
				// (2) previous execution has failed before first checkpoint completed
				//
				// in case of (2) we have to abort all previous transactions
				abortTransactions(transactionalIdsGenerator.generateIdsToAbort());
			} else {
				nextTransactionalIdHint = transactionalIdHints.get(0);
			}
		}

		super.initializeState(context);
	}

	@Override
	protected Optional<KafkaTransactionContext> initializeUserContext() {
		if (semantic != Semantic.EXACTLY_ONCE) {
			return Optional.empty();
		}

		Set<String> transactionalIds = generateNewTransactionalIds();
		resetAvailableTransactionalIdsPool(transactionalIds);
		return Optional.of(new KafkaTransactionContext(transactionalIds));
	}

	private Set<String> generateNewTransactionalIds() {
		checkState(nextTransactionalIdHint != null, "nextTransactionalIdHint must be present for EXACTLY_ONCE");

		Set<String> transactionalIds = transactionalIdsGenerator.generateIdsToUse(nextTransactionalIdHint.nextFreeTransactionalId);
		LOG.info("Generated new transactionalIds {}", transactionalIds);
		return transactionalIds;
	}

	@Override
	protected void finishRecoveringContext() {
		cleanUpUserContext();
		resetAvailableTransactionalIdsPool(getUserContext().get().transactionalIds);
		LOG.info("Recovered transactionalIds {}", getUserContext().get().transactionalIds);
	}

	/**
	 * After initialization make sure that all previous transactions from the current user context have been completed.
	 */
	private void cleanUpUserContext() {
		if (!getUserContext().isPresent()) {
			return;
		}
		abortTransactions(getUserContext().get().transactionalIds);
	}

	private void resetAvailableTransactionalIdsPool(Collection<String> transactionalIds) {
		availableTransactionalIds.clear();
		for (String transactionalId : transactionalIds) {
			availableTransactionalIds.add(transactionalId);
		}
	}

	// ----------------------------------- Utilities --------------------------

	private void abortTransactions(Set<String> transactionalIds) {
		for (String transactionalId : transactionalIds) {
			try (FlinkKafkaProducer<byte[], byte[]> kafkaProducer =
					initTransactionalProducer(transactionalId, false)) {
				kafkaProducer.initTransactions();
			}
		}
	}

	int getTransactionCoordinatorId() {
		final KafkaTransactionState currentTransaction = currentTransaction();
		if (currentTransaction == null || currentTransaction.producer == null) {
			throw new IllegalArgumentException();
		}
		return currentTransaction.producer.getTransactionCoordinatorId();
	}

	/**
	 * For each checkpoint we create new {@link FlinkKafkaProducer} so that new transactions will not clash
	 * with transactions created during previous checkpoints ({@code producer.initTransactions()} assures that we
	 * obtain new producerId and epoch counters).
	 */
	private FlinkKafkaProducer<byte[], byte[]> createTransactionalProducer() throws FlinkKafka011Exception {
		String transactionalId = availableTransactionalIds.poll();
		if (transactionalId == null) {
			throw new FlinkKafka011Exception(
				FlinkKafka011ErrorCode.PRODUCERS_POOL_EMPTY,
				"Too many ongoing snapshots. Increase kafka producers pool size or decrease number of concurrent checkpoints.");
		}
		FlinkKafkaProducer<byte[], byte[]> producer = initTransactionalProducer(transactionalId, true);
		producer.initTransactions();
		return producer;
	}

	private void recycleTransactionalProducer(FlinkKafkaProducer<byte[], byte[]> producer) {
		availableTransactionalIds.add(producer.getTransactionalId());
		producer.close();
	}

	private FlinkKafkaProducer<byte[], byte[]> initTransactionalProducer(String transactionalId, boolean registerMetrics) {
		producerConfig.put("transactional.id", transactionalId);
		return initProducer(registerMetrics);
	}

	private FlinkKafkaProducer<byte[], byte[]> initProducer(boolean registerMetrics) {
		FlinkKafkaProducer<byte[], byte[]> producer = new FlinkKafkaProducer<>(this.producerConfig);

		RuntimeContext ctx = getRuntimeContext();

		if (flinkKafkaPartitioner != null) {
			if (flinkKafkaPartitioner instanceof FlinkKafkaDelegatePartitioner) {
				((FlinkKafkaDelegatePartitioner) flinkKafkaPartitioner).setPartitions(
					getPartitionsByTopic(this.defaultTopicId, producer));
			}
			flinkKafkaPartitioner.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
		}

		LOG.info("Starting FlinkKafkaProducer ({}/{}) to produce into default topic {}",
			ctx.getIndexOfThisSubtask() + 1, ctx.getNumberOfParallelSubtasks(), defaultTopicId);

		// register Kafka metrics to Flink accumulators
		if (registerMetrics && !Boolean.parseBoolean(producerConfig.getProperty(KEY_DISABLE_METRICS, "false"))) {
			Map<MetricName, ? extends Metric> metrics = producer.metrics();

			if (metrics == null) {
				// MapR's Kafka implementation returns null here.
				LOG.info("Producer implementation does not support metrics");
			} else {
				final MetricGroup kafkaMetricGroup = getRuntimeContext().getMetricGroup().addGroup("KafkaProducer");
				for (Map.Entry<MetricName, ? extends Metric> entry: metrics.entrySet()) {
					String name = entry.getKey().name();
					Metric metric = entry.getValue();

					KafkaMetricMuttableWrapper wrapper = previouslyCreatedMetrics.get(name);
					if (wrapper != null) {
						wrapper.setKafkaMetric(metric);
					} else {
						// TODO: somehow merge metrics from all active producers?
						wrapper = new KafkaMetricMuttableWrapper(metric);
						previouslyCreatedMetrics.put(name, wrapper);
						kafkaMetricGroup.gauge(name, wrapper);
					}
				}
			}
		}
		return producer;
	}

	private void checkErroneous() throws FlinkKafka011Exception {
		Exception e = asyncException;
		if (e != null) {
			// prevent double throwing
			asyncException = null;
			throw new FlinkKafka011Exception(
				FlinkKafka011ErrorCode.EXTERNAL_ERROR,
				"Failed to send data to Kafka: " + e.getMessage(),
				e);
		}
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
	}

	private static Properties getPropertiesFromBrokerList(String brokerList) {
		String[] elements = brokerList.split(",");

		// validate the broker addresses
		for (String broker: elements) {
			NetUtils.getCorrectHostnamePort(broker);
		}

		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		return props;
	}

	private static int[] getPartitionsByTopic(String topic, Producer<byte[], byte[]> producer) {
		// the fetched list is immutable, so we're creating a mutable copy in order to sort it
		List<PartitionInfo> partitionsList = new ArrayList<>(producer.partitionsFor(topic));

		// sort the partitions by partition id to make sure the fetched partition list is the same across subtasks
		Collections.sort(partitionsList, new Comparator<PartitionInfo>() {
			@Override
			public int compare(PartitionInfo o1, PartitionInfo o2) {
				return Integer.compare(o1.partition(), o2.partition());
			}
		});

		int[] partitions = new int[partitionsList.size()];
		for (int i = 0; i < partitions.length; i++) {
			partitions[i] = partitionsList.get(i).partition();
		}

		return partitions;
	}

	/**
	 * State for handling transactions.
	 */
	@VisibleForTesting
	@Internal
	static class KafkaTransactionState {

		private final transient FlinkKafkaProducer<byte[], byte[]> producer;

		@Nullable
		final String transactionalId;

		final long producerId;

		final short epoch;

		KafkaTransactionState(String transactionalId, FlinkKafkaProducer<byte[], byte[]> producer) {
			this(transactionalId, producer.getProducerId(), producer.getEpoch(), producer);
		}

		KafkaTransactionState(FlinkKafkaProducer<byte[], byte[]> producer) {
			this(null, -1, (short) -1, producer);
		}

		KafkaTransactionState(
				String transactionalId,
				long producerId,
				short epoch,
				FlinkKafkaProducer<byte[], byte[]> producer) {
			this.transactionalId = transactionalId;
			this.producerId = producerId;
			this.epoch = epoch;
			this.producer = producer;
		}

		@Override
		public String toString() {
			return String.format(
				"%s [transactionalId=%s, producerId=%s, epoch=%s]",
				this.getClass().getSimpleName(),
				transactionalId,
				producerId,
				epoch);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			KafkaTransactionState that = (KafkaTransactionState) o;

			if (producerId != that.producerId) {
				return false;
			}
			if (epoch != that.epoch) {
				return false;
			}
			return transactionalId != null ? transactionalId.equals(that.transactionalId) : that.transactionalId == null;
		}

		@Override
		public int hashCode() {
			int result = transactionalId != null ? transactionalId.hashCode() : 0;
			result = 31 * result + (int) (producerId ^ (producerId >>> 32));
			result = 31 * result + (int) epoch;
			return result;
		}
	}

	/**
	 * Context associated to this instance of the {@link FlinkKafkaProducer011}. User for keeping track of the
	 * transactionalIds.
	 */
	@VisibleForTesting
	@Internal
	public static class KafkaTransactionContext {
		final Set<String> transactionalIds;

		KafkaTransactionContext(Set<String> transactionalIds) {
			checkNotNull(transactionalIds);
			this.transactionalIds = transactionalIds;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			KafkaTransactionContext that = (KafkaTransactionContext) o;

			return transactionalIds.equals(that.transactionalIds);
		}

		@Override
		public int hashCode() {
			return transactionalIds.hashCode();
		}
	}

	/**
	 * {@link org.apache.flink.api.common.typeutils.TypeSerializer} for
	 * {@link KafkaTransactionState}.
	 */
	@VisibleForTesting
	@Internal
	public static class TransactionStateSerializer extends TypeSerializerSingleton<KafkaTransactionState> {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public KafkaTransactionState createInstance() {
			return null;
		}

		@Override
		public KafkaTransactionState copy(KafkaTransactionState from) {
			return from;
		}

		@Override
		public KafkaTransactionState copy(
			KafkaTransactionState from,
			KafkaTransactionState reuse) {
			return from;
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(
				KafkaTransactionState record,
				DataOutputView target) throws IOException {
			if (record.transactionalId == null) {
				target.writeBoolean(false);
			} else {
				target.writeBoolean(true);
				target.writeUTF(record.transactionalId);
			}
			target.writeLong(record.producerId);
			target.writeShort(record.epoch);
		}

		@Override
		public KafkaTransactionState deserialize(DataInputView source) throws IOException {
			String transactionalId = null;
			if (source.readBoolean()) {
				transactionalId = source.readUTF();
			}
			long producerId = source.readLong();
			short epoch = source.readShort();
			return new KafkaTransactionState(transactionalId, producerId, epoch, null);
		}

		@Override
		public KafkaTransactionState deserialize(
				KafkaTransactionState reuse,
				DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(
				DataInputView source, DataOutputView target) throws IOException {
			boolean hasTransactionalId = source.readBoolean();
			target.writeBoolean(hasTransactionalId);
			if (hasTransactionalId) {
				target.writeUTF(source.readUTF());
			}
			target.writeLong(source.readLong());
			target.writeShort(source.readShort());
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof TransactionStateSerializer;
		}
	}

	/**
	 * {@link org.apache.flink.api.common.typeutils.TypeSerializer} for
	 * {@link KafkaTransactionContext}.
	 */
	@VisibleForTesting
	@Internal
	public static class ContextStateSerializer extends TypeSerializerSingleton<KafkaTransactionContext> {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public KafkaTransactionContext createInstance() {
			return null;
		}

		@Override
		public KafkaTransactionContext copy(KafkaTransactionContext from) {
			return from;
		}

		@Override
		public KafkaTransactionContext copy(
				KafkaTransactionContext from,
				KafkaTransactionContext reuse) {
			return from;
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(
				KafkaTransactionContext record,
				DataOutputView target) throws IOException {
			int numIds = record.transactionalIds.size();
			target.writeInt(numIds);
			for (String id : record.transactionalIds) {
				target.writeUTF(id);
			}
		}

		@Override
		public KafkaTransactionContext deserialize(DataInputView source) throws IOException {
			int numIds = source.readInt();
			Set<String> ids = new HashSet<>(numIds);
			for (int i = 0; i < numIds; i++) {
				ids.add(source.readUTF());
			}
			return new KafkaTransactionContext(ids);
		}

		@Override
		public KafkaTransactionContext deserialize(
				KafkaTransactionContext reuse,
				DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(
				DataInputView source,
				DataOutputView target) throws IOException {
			int numIds = source.readInt();
			target.writeInt(numIds);
			for (int i = 0; i < numIds; i++) {
				target.writeUTF(source.readUTF());
			}
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof ContextStateSerializer;
		}
	}

	/**
	 * Keep information required to deduce next safe to use transactional id.
	 */
	public static class NextTransactionalIdHint {
		public int lastParallelism = 0;
		public long nextFreeTransactionalId = 0;

		public NextTransactionalIdHint() {
			this(0, 0);
		}

		public NextTransactionalIdHint(int parallelism, long nextFreeTransactionalId) {
			this.lastParallelism = parallelism;
			this.nextFreeTransactionalId = nextFreeTransactionalId;
		}
	}
}
