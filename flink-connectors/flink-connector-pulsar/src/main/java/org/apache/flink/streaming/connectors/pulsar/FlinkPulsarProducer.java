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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pulsar.partitioner.PulsarKeyExtractor;
import org.apache.flink.util.SerializableObject;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Flink Sink to produce data into a Pulsar topic.
 */
public class FlinkPulsarProducer<IN>
		extends RichSinkFunction<IN>
		implements CheckpointedFunction {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkPulsarProducer.class);

	/**
	 * The pulsar service url.
	 */
	protected final String serviceUrl;

	/**
	 * User defined configuration for the producer.
	 */
	protected final ProducerConfiguration producerConfig;

	/**
	 * The name of the default topic this producer is writing data to.
	 */
	protected final String defaultTopicName;

	/**
	 * (Serializable) SerializationSchema for turning objects used with Flink into.
	 * byte[] for Pulsar.
	 */
	protected final SerializationSchema<IN> schema;

	/**
	 * User-provided key extractor for assigning a key to a pulsar message.
	 */
	protected final PulsarKeyExtractor<IN> flinkPulsarKeyExtractor;

	/**
	 * Produce Mode.
	 */
	protected PulsarProduceMode produceMode = PulsarProduceMode.AT_LEAST_ONE;

	/**
	 * If true, the producer will wait until all outstanding records have been send to the broker.
	 */
	protected boolean flushOnCheckpoint;

	// -------------------------------- Runtime fields ------------------------------------------

	/** KafkaProducer instance. */
	protected transient Producer producer;

	/** The callback than handles error propagation or logging callbacks. */
	protected transient Function<MessageId, MessageId> successCallback = msgId -> {
		acknowledgeMessage();
		return msgId;
	};

	protected transient Function<Throwable, MessageId> failureCallback;

	/** Errors encountered in the async producer are stored here. */
	protected transient volatile Exception asyncException;

	/** Lock for accessing the pending records. */
	protected final SerializableObject pendingRecordsLock = new SerializableObject();

	/** Number of unacknowledged records. */
	protected long pendingRecords;

	public FlinkPulsarProducer(String serviceUrl,
							String defaultTopicName,
							SerializationSchema<IN> serializationSchema,
							ProducerConfiguration producerConfig,
							PulsarKeyExtractor<IN> keyExtractor) {
		this.serviceUrl = checkNotNull(serviceUrl, "Service url not set");
		this.defaultTopicName = checkNotNull(defaultTopicName, "TopicName not set");
		this.schema = checkNotNull(serializationSchema, "Serialization Schema not set");
		this.producerConfig = checkNotNull(producerConfig, "Producer Config is not set");
		this.flinkPulsarKeyExtractor = getOrNullKeyExtractor(keyExtractor);
		ClosureCleaner.ensureSerializable(serializationSchema);
	}

	// ---------------------------------- Properties --------------------------


	/**
	 * @return pulsar key extractor.
	 */
	public PulsarKeyExtractor<IN> getKeyExtractor() {
		return flinkPulsarKeyExtractor;
	}

	/**
	 * Gets this producer's operating mode.
	 */
	public PulsarProduceMode getProduceMode() {
		return this.produceMode;
	}

	/**
	 * Sets this producer's operating mode.
	 *
	 * @param produceMode The mode of operation.
	 */
	public void setProduceMode(PulsarProduceMode produceMode) {
		this.produceMode = checkNotNull(produceMode);
	}

	/**
	 * If set to true, the Flink producer will wait for all outstanding messages in the Pulsar buffers
	 * to be acknowledged by the Pulsar producer on a checkpoint.
	 * This way, the producer can guarantee that messages in the Pulsar buffers are part of the checkpoint.
	 *
	 * @param flush Flag indicating the flushing mode (true = flush on checkpoint)
	 */
	public void setFlushOnCheckpoint(boolean flush) {
		this.flushOnCheckpoint = flush;
	}

	// ----------------------------------- Sink Methods --------------------------

	@SuppressWarnings("unchecked")
	private static final <T> PulsarKeyExtractor<T> getOrNullKeyExtractor(PulsarKeyExtractor<T> extractor) {
		if (null == extractor) {
			return PulsarKeyExtractor.NULL;
		} else {
			return extractor;
		}
	}

	private Producer createProducer(ProducerConfiguration configuration) throws Exception {
		PulsarClient client = PulsarClient.create(serviceUrl);
		return client.createProducer(defaultTopicName, configuration);
	}

	/**
	 * Initializes the connection to pulsar.
	 *
	 * @param parameters configuration used for initialization
	 * @throws Exception
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		this.producer = createProducer(producerConfig);

		RuntimeContext ctx = getRuntimeContext();

		LOG.info("Starting FlinkPulsarProducer ({}/{}) to produce into pulsar topic {}",
			ctx.getIndexOfThisSubtask() + 1, ctx.getNumberOfParallelSubtasks(), defaultTopicName);

		if (flushOnCheckpoint && !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
			LOG.warn("Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
			flushOnCheckpoint = false;
		}

		if (PulsarProduceMode.AT_MOST_ONCE == produceMode) {
			this.failureCallback = cause -> {
				LOG.error("Error while sending record to Pulsar : " + cause.getMessage(), cause);
				return null;
			};
		} else if (PulsarProduceMode.AT_LEAST_ONE == produceMode){
			this.failureCallback = cause -> {
				if (null == asyncException) {
					if (cause instanceof Exception) {
						asyncException = (Exception) cause;
					} else {
						asyncException = new Exception(cause);
					}
				}
				return null;
			};
		} else {
			throw new UnsupportedOperationException("Unsupported produce mode " + produceMode);
		}
	}

	@Override
	public void invoke(IN value, Context context) throws Exception {
		checkErroneous();

		byte[] serializedValue = schema.serialize(value);

		MessageBuilder msgBuilder = MessageBuilder.create();
		if (null != context.timestamp()) {
			msgBuilder = msgBuilder.setEventTime(context.timestamp());
		}
		String msgKey = flinkPulsarKeyExtractor.getKey(value);
		if (null != msgKey) {
			msgBuilder = msgBuilder.setKey(msgKey);
		}
		Message message = msgBuilder
			.setContent(serializedValue)
			.build();

		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords++;
			}
		}
		producer.sendAsync(message)
			.thenApply(successCallback)
			.exceptionally(failureCallback);
	}

	@Override
	public void close() throws Exception {
		if (producer != null) {
			producer.close();
		}

		// make sure we propagate pending errors
		checkErroneous();
	}

	// ------------------- Logic for handling checkpoint flushing -------------------------- //

	private void acknowledgeMessage() {
		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords--;
				if (pendingRecords == 0) {
					pendingRecordsLock.notifyAll();
				}
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		// check for asynchronous errors and fail the checkpoint if necessary
		checkErroneous();

		if (flushOnCheckpoint) {
			// wait until all the messages are acknowledged
			synchronized (pendingRecordsLock) {
				while (pendingRecords > 0) {
					pendingRecordsLock.wait(100);
				}
			}

			// if the flushed requests has errors, we should propagate it also and fail the checkpoint
			checkErroneous();
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// nothing to do
	}

	// ----------------------------------- Utilities --------------------------

	protected void checkErroneous() throws Exception {
		Exception e = asyncException;
		if (e != null) {
			// prevent double throwing
			asyncException = null;
			throw new Exception("Failed to send data to Kafka: " + e.getMessage(), e);
		}
	}

}
