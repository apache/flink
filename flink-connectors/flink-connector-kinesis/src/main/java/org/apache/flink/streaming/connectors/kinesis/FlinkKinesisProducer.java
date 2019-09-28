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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;
import org.apache.flink.streaming.connectors.kinesis.util.TimeoutLatch;
import org.apache.flink.util.InstantiationUtil;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The FlinkKinesisProducer allows to produce from a Flink DataStream into Kinesis.
 *
 * @param <OUT> Data type to produce into Kinesis Streams
 */
@PublicEvolving
public class FlinkKinesisProducer<OUT> extends RichSinkFunction<OUT> implements CheckpointedFunction {

	public static final String KINESIS_PRODUCER_METRIC_GROUP = "kinesisProducer";

	public static final String METRIC_BACKPRESSURE_CYCLES = "backpressureCycles";

	public static final String METRIC_OUTSTANDING_RECORDS_COUNT = "outstandingRecordsCount";

	private static final long serialVersionUID = 6447077318449477846L;

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKinesisProducer.class);

	/** Properties to parametrize settings such as AWS service region, access key etc. */
	private final Properties configProps;

	/* Flag controlling the error behavior of the producer */
	private boolean failOnError = false;

	/* Maximum length of the internal record queue before backpressuring */
	private int queueLimit = Integer.MAX_VALUE;

	/* Name of the default stream to produce to. Can be overwritten by the serialization schema */
	private String defaultStream;

	/* Default partition id. Can be overwritten by the serialization schema */
	private String defaultPartition;

	/* Schema for turning the OUT type into a byte array. */
	private final KinesisSerializationSchema<OUT> schema;

	/* Optional custom partitioner */
	private KinesisPartitioner<OUT> customPartitioner = null;

	// --------------------------- Runtime fields ---------------------------

	/* Our Kinesis instance for each parallel Flink sink */
	private transient KinesisProducer producer;

	/* Backpressuring waits for this latch, triggered by record callback */
	private transient TimeoutLatch backpressureLatch;

	/* Callback handling failures */
	private transient FutureCallback<UserRecordResult> callback;

	/* Counts how often we have to wait for KPL because we are above the queue limit */
	private transient Counter backpressureCycles;

	/* Field for async exception */
	private transient volatile Throwable thrownException;

	// --------------------------- Initialization and configuration  ---------------------------

	/**
	 * Create a new FlinkKinesisProducer.
	 * This is a constructor supporting Flink's {@see SerializationSchema}.
	 *
	 * @param schema Serialization schema for the data type
	 * @param configProps The properties used to configure KinesisProducer, including AWS credentials and AWS region
	 */
	public FlinkKinesisProducer(final SerializationSchema<OUT> schema, Properties configProps) {

		// create a simple wrapper for the serialization schema
		this(new KinesisSerializationSchema<OUT>() {
			@Override
			public ByteBuffer serialize(OUT element) {
				// wrap into ByteBuffer
				return ByteBuffer.wrap(schema.serialize(element));
			}
			// use default stream and hash key

			@Override
			public String getTargetStream(OUT element) {
				return null;
			}
		}, configProps);
	}

	/**
	 * Create a new FlinkKinesisProducer.
	 * This is a constructor supporting {@see KinesisSerializationSchema}.
	 *
	 * @param schema Kinesis serialization schema for the data type
	 * @param configProps The properties used to configure KinesisProducer, including AWS credentials and AWS region
	 */
	public FlinkKinesisProducer(KinesisSerializationSchema<OUT> schema, Properties configProps) {
		checkNotNull(configProps, "configProps can not be null");
		this.configProps = KinesisConfigUtil.replaceDeprecatedProducerKeys(configProps);

		checkNotNull(schema, "serialization schema cannot be null");
		checkArgument(
			InstantiationUtil.isSerializable(schema),
			"The provided serialization schema is not serializable: " + schema.getClass().getName() + ". " +
				"Please check that it does not contain references to non-serializable instances.");
		this.schema = schema;
	}

	/**
	 * If set to true, the producer will immediately fail with an exception on any error.
	 * Otherwise, the errors are logged and the producer goes on.
	 *
	 * @param failOnError Error behavior flag
	 */
	public void setFailOnError(boolean failOnError) {
		this.failOnError = failOnError;
	}

	/**
	 * The {@link KinesisProducer} holds an unbounded queue internally. To avoid memory
	 * problems under high loads, a limit can be employed above which the internal queue
	 * will be flushed, thereby applying backpressure.
	 *
	 * @param queueLimit The maximum length of the internal queue before backpressuring
	 */
	public void setQueueLimit(int queueLimit) {
		checkArgument(queueLimit > 0, "queueLimit must be a positive number");
		this.queueLimit = queueLimit;
	}

	/**
	 * Set a default stream name.
	 * @param defaultStream Name of the default Kinesis stream
	 */
	public void setDefaultStream(String defaultStream) {
		this.defaultStream = defaultStream;
	}

	/**
	 * Set default partition id.
	 * @param defaultPartition Name of the default partition
	 */
	public void setDefaultPartition(String defaultPartition) {
		this.defaultPartition = defaultPartition;
	}

	public void setCustomPartitioner(KinesisPartitioner<OUT> partitioner) {
		checkNotNull(partitioner, "partitioner cannot be null");
		checkArgument(
			InstantiationUtil.isSerializable(partitioner),
			"The provided custom partitioner is not serializable: " + partitioner.getClass().getName() + ". " +
				"Please check that it does not contain references to non-serializable instances.");

		this.customPartitioner = partitioner;
	}

	// --------------------------- Lifecycle methods ---------------------------

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		// check and pass the configuration properties
		KinesisProducerConfiguration producerConfig = KinesisConfigUtil.getValidatedProducerConfiguration(configProps);

		producer = getKinesisProducer(producerConfig);

		final MetricGroup kinesisMectricGroup = getRuntimeContext().getMetricGroup().addGroup(KINESIS_PRODUCER_METRIC_GROUP);
		this.backpressureCycles = kinesisMectricGroup.counter(METRIC_BACKPRESSURE_CYCLES);
		kinesisMectricGroup.gauge(METRIC_OUTSTANDING_RECORDS_COUNT, producer::getOutstandingRecordsCount);

		backpressureLatch = new TimeoutLatch();
		callback = new FutureCallback<UserRecordResult>() {
			@Override
			public void onSuccess(UserRecordResult result) {
				backpressureLatch.trigger();
				if (!result.isSuccessful()) {
					if (failOnError) {
						// only remember the first thrown exception
						if (thrownException == null) {
							thrownException = new RuntimeException("Record was not sent successful");
						}
					} else {
						LOG.warn("Record was not sent successful");
					}
				}
			}

			@Override
			public void onFailure(Throwable t) {
				backpressureLatch.trigger();
				if (failOnError) {
					thrownException = t;
				} else {
					LOG.warn("An exception occurred while processing a record", t);
				}
			}
		};

		if (this.customPartitioner != null) {
			this.customPartitioner.initialize(getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
		}

		LOG.info("Started Kinesis producer instance for region '{}'", producerConfig.getRegion());
	}

	@Override
	public void invoke(OUT value, Context context) throws Exception {
		if (this.producer == null) {
			throw new RuntimeException("Kinesis producer has been closed");
		}

		checkAndPropagateAsyncError();
		boolean didWaitForFlush = enforceQueueLimit();

		if (didWaitForFlush) {
			checkAndPropagateAsyncError();
		}

		String stream = defaultStream;
		String partition = defaultPartition;

		ByteBuffer serialized = schema.serialize(value);

		// maybe set custom stream
		String customStream = schema.getTargetStream(value);
		if (customStream != null) {
			stream = customStream;
		}

		String explicitHashkey = null;
		// maybe set custom partition
		if (customPartitioner != null) {
			partition = customPartitioner.getPartitionId(value);
			explicitHashkey = customPartitioner.getExplicitHashKey(value);
		}

		if (stream == null) {
			if (failOnError) {
				throw new RuntimeException("No target stream set");
			} else {
				LOG.warn("No target stream set. Skipping record");
				return;
			}
		}

		ListenableFuture<UserRecordResult> cb = producer.addUserRecord(stream, partition, explicitHashkey, serialized);
		Futures.addCallback(cb, callback);
	}

	@Override
	public void close() throws Exception {
		LOG.info("Closing producer");
		super.close();

		if (producer != null) {
			LOG.info("Flushing outstanding {} records", producer.getOutstandingRecordsCount());
			// try to flush all outstanding records
			flushSync();

			LOG.info("Flushing done. Destroying producer instance.");
			producer.destroy();
			producer = null;
		}

		// make sure we propagate pending errors
		checkAndPropagateAsyncError();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// nothing to do
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		// check for asynchronous errors and fail the checkpoint if necessary
		checkAndPropagateAsyncError();

		flushSync();
		if (producer.getOutstandingRecordsCount() > 0) {
			throw new IllegalStateException(
				"Number of outstanding records must be zero at this point: " + producer.getOutstandingRecordsCount());
		}

		// if the flushed requests has errors, we should propagate it also and fail the checkpoint
		checkAndPropagateAsyncError();
	}

	// --------------------------- Utilities ---------------------------

	/**
	 * Creates a {@link KinesisProducer}.
	 * Exposed so that tests can inject mock producers easily.
	 */
	@VisibleForTesting
	protected KinesisProducer getKinesisProducer(KinesisProducerConfiguration producerConfig) {
		return new KinesisProducer(producerConfig);
	}

	/**
	 * Check if there are any asynchronous exceptions. If so, rethrow the exception.
	 */
	private void checkAndPropagateAsyncError() throws Exception {
		if (thrownException != null) {
			String errorMessages = "";
			if (thrownException instanceof UserRecordFailedException) {
				List<Attempt> attempts = ((UserRecordFailedException) thrownException).getResult().getAttempts();
				for (Attempt attempt: attempts) {
					if (attempt.getErrorMessage() != null) {
						errorMessages += attempt.getErrorMessage() + "\n";
					}
				}
			}
			if (failOnError) {
				throw new RuntimeException("An exception was thrown while processing a record: " + errorMessages, thrownException);
			} else {
				LOG.warn("An exception was thrown while processing a record: {}", thrownException, errorMessages);

				// reset, prevent double throwing
				thrownException = null;
			}
		}
	}

	/**
	 * If the internal queue of the {@link KinesisProducer} gets too long,
	 * flush some of the records until we are below the limit again.
	 * We don't want to flush _all_ records at this point since that would
	 * break record aggregation.
	 *
	 * @return boolean whether flushing occurred or not
	 */
	private boolean enforceQueueLimit() {
		int attempt = 0;
		while (producer.getOutstandingRecordsCount() >= queueLimit) {
			backpressureCycles.inc();
			if (attempt >= 10) {
				LOG.warn("Waiting for the queue length to drop below the limit takes unusually long, still not done after {} attempts.", attempt);
			}
			attempt++;
			try {
				backpressureLatch.await(100);
			} catch (InterruptedException e) {
				LOG.warn("Flushing was interrupted.");
				break;
			}
		}
		return attempt > 0;
	}

	/**
	 * A reimplementation of {@link KinesisProducer#flushSync()}.
	 * This implementation releases the block on flushing if an interruption occurred.
	 */
	private void flushSync() throws Exception {
		while (producer.getOutstandingRecordsCount() > 0) {
			producer.flush();
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				LOG.warn("Flushing was interrupted.");
				break;
			}
		}
	}
}
