/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Internal flink kafka producer.
 */
@PublicEvolving
public class FlinkKafkaInternalProducer<K, V> implements Producer<K, V> {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaInternalProducer.class);

	protected final KafkaProducer<K, V> kafkaProducer;

	// This lock and closed flag are introduced to workaround KAFKA-6635. Because the bug is only fixed in
	// Kafka 2.3.0, we need this workaround before Kafka dependency is bumped to 2.3.0 to avoid deadlock
	// between a transaction committing / aborting thread and a producer closing thread.
	// TODO: remove the workaround after Kafka dependency is bumped to 2.3.0+
	private final Object producerClosingLock;
	private volatile boolean closed;

	@Nullable
	protected final String transactionalId;

	public FlinkKafkaInternalProducer(Properties properties) {
		transactionalId = properties.getProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
		kafkaProducer = new KafkaProducer<>(properties);
		producerClosingLock = new Object();
		closed = false;
	}

	// -------------------------------- Simple proxy method calls --------------------------------

	@Override
	public void initTransactions() {
		synchronized (producerClosingLock) {
			ensureNotClosed();
			kafkaProducer.initTransactions();
		}
	}

	@Override
	public void beginTransaction() throws ProducerFencedException {
		synchronized (producerClosingLock) {
			ensureNotClosed();
			kafkaProducer.beginTransaction();
		}
	}

	@Override
	public void commitTransaction() throws ProducerFencedException {
		synchronized (producerClosingLock) {
			ensureNotClosed();
			kafkaProducer.commitTransaction();
		}
	}

	@Override
	public void abortTransaction() throws ProducerFencedException {
		synchronized (producerClosingLock) {
			ensureNotClosed();
			kafkaProducer.abortTransaction();
		}
	}

	@Override
	public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {
		synchronized (producerClosingLock) {
			ensureNotClosed();
			kafkaProducer.sendOffsetsToTransaction(offsets, consumerGroupId);
		}
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
		return kafkaProducer.send(record);
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
		return kafkaProducer.send(record, callback);
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		synchronized (producerClosingLock) {
			ensureNotClosed();
			return kafkaProducer.partitionsFor(topic);
		}
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return kafkaProducer.metrics();
	}

	@Override
	public void close() {
		throw new UnsupportedOperationException("Close without timeout is now allowed because it can leave lingering Kafka threads.");
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		synchronized (producerClosingLock) {
			kafkaProducer.close(timeout, unit);
			if (LOG.isDebugEnabled()) {
				LOG.debug(
						"Closed internal KafkaProducer {}. Stacktrace: {}",
						System.identityHashCode(this),
						Joiner.on("\n").join(Thread.currentThread().getStackTrace()));
			}
			closed = true;
		}
	}

	@Override
	public void close(Duration duration) {
		synchronized (producerClosingLock) {
			kafkaProducer.close(duration);
			if (LOG.isDebugEnabled()) {
				LOG.debug(
						"Closed internal KafkaProducer {}. Stacktrace: {}",
						System.identityHashCode(this),
						Joiner.on("\n").join(Thread.currentThread().getStackTrace()));
			}
			closed = true;
		}
	}

	// -------------------------------- New methods or methods with changed behaviour --------------------------------

	@Override
	public void flush() {
		kafkaProducer.flush();
		if (transactionalId != null) {
			synchronized (producerClosingLock) {
				ensureNotClosed();
				flushNewPartitions();
			}
		}
	}

	/**
	 * Instead of obtaining producerId and epoch from the transaction coordinator, re-use previously obtained ones,
	 * so that we can resume transaction after a restart. Implementation of this method is based on
	 * {@link KafkaProducer#initTransactions}.
	 * https://github.com/apache/kafka/commit/5d2422258cb975a137a42a4e08f03573c49a387e#diff-f4ef1afd8792cd2a2e9069cd7ddea630
	 */
	public void resumeTransaction(long producerId, short epoch) {
		synchronized (producerClosingLock) {
			ensureNotClosed();
			Preconditions.checkState(producerId >= 0 && epoch >= 0,
				"Incorrect values for producerId %s and epoch %s",
				producerId,
				epoch);
			LOG.info("Attempting to resume transaction {} with producerId {} and epoch {}",
				transactionalId,
				producerId,
				epoch);

			Object transactionManager = getField(kafkaProducer, "transactionManager");
			synchronized (transactionManager) {
				Object topicPartitionBookkeeper =
						getField(transactionManager, "topicPartitionBookkeeper");

				invoke(transactionManager,
					"transitionTo",
					getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.INITIALIZING"));
				invoke(topicPartitionBookkeeper, "reset");

				Object producerIdAndEpoch = getField(transactionManager, "producerIdAndEpoch");
				setField(producerIdAndEpoch, "producerId", producerId);
				setField(producerIdAndEpoch, "epoch", epoch);

				invoke(transactionManager,
					"transitionTo",
					getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.READY"));

				invoke(transactionManager,
					"transitionTo",
					getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.IN_TRANSACTION"));
				setField(transactionManager, "transactionStarted", true);
			}
		}
	}

	public String getTransactionalId() {
		return transactionalId;
	}

	public long getProducerId() {
		Object transactionManager = getField(kafkaProducer, "transactionManager");
		Object producerIdAndEpoch = getField(transactionManager, "producerIdAndEpoch");
		return (long) getField(producerIdAndEpoch, "producerId");
	}

	public short getEpoch() {
		Object transactionManager = getField(kafkaProducer, "transactionManager");
		Object producerIdAndEpoch = getField(transactionManager, "producerIdAndEpoch");
		return (short) getField(producerIdAndEpoch, "epoch");
	}

	@VisibleForTesting
	public int getTransactionCoordinatorId() {
		Object transactionManager = getField(kafkaProducer, "transactionManager");
		Node node = (Node) invoke(transactionManager, "coordinator", FindCoordinatorRequest.CoordinatorType.TRANSACTION);
		return node.id();
	}

	private void ensureNotClosed() {
		if (closed) {
			throw new IllegalStateException(String.format("The producer %s has already been closed", System.identityHashCode(this)));
		}
	}

	/**
	 * Besides committing {@link org.apache.kafka.clients.producer.KafkaProducer#commitTransaction} is also adding new
	 * partitions to the transaction. flushNewPartitions method is moving this logic to pre-commit/flush, to make
	 * resumeTransaction simpler. Otherwise resumeTransaction would require to restore state of the not yet added/"in-flight"
	 * partitions.
	 */
	private void flushNewPartitions() {
		LOG.info("Flushing new partitions");
		TransactionalRequestResult result = enqueueNewPartitions();
		Object sender = getField(kafkaProducer, "sender");
		invoke(sender, "wakeup");
		result.await();
	}

	/**
	 * Enqueues new transactions at the transaction manager and returns a {@link
	 * TransactionalRequestResult} that allows waiting on them.
	 *
	 * <p>If there are no new transactions we return a {@link TransactionalRequestResult} that is
	 * already done.
	 */
	private TransactionalRequestResult enqueueNewPartitions() {
		Object transactionManager = getField(kafkaProducer, "transactionManager");
		synchronized (transactionManager) {
			Object newPartitionsInTransaction = getField(transactionManager, "newPartitionsInTransaction");
			Object newPartitionsInTransactionIsEmpty = invoke(newPartitionsInTransaction, "isEmpty");
			TransactionalRequestResult result;
			if (newPartitionsInTransactionIsEmpty instanceof Boolean && !((Boolean) newPartitionsInTransactionIsEmpty)) {
				Object txnRequestHandler = invoke(transactionManager, "addPartitionsToTransactionHandler");
				invoke(transactionManager, "enqueueRequest", new Class[]{txnRequestHandler.getClass().getSuperclass()}, new Object[]{txnRequestHandler});
				result = (TransactionalRequestResult) getField(txnRequestHandler, txnRequestHandler.getClass().getSuperclass(), "result");
			} else {
				// we don't have an operation but this operation string is also used in
				// addPartitionsToTransactionHandler.
				result = new TransactionalRequestResult("AddPartitionsToTxn");
				result.done();
			}
			return result;
		}
	}

	protected static Enum<?> getEnum(String enumFullName) {
		String[] x = enumFullName.split("\\.(?=[^\\.]+$)");
		if (x.length == 2) {
			String enumClassName = x[0];
			String enumName = x[1];
			try {
				Class<Enum> cl = (Class<Enum>) Class.forName(enumClassName);
				return Enum.valueOf(cl, enumName);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Incompatible KafkaProducer version", e);
			}
		}
		return null;
	}

	protected static Object invoke(Object object, String methodName, Object... args) {
		Class<?>[] argTypes = new Class[args.length];
		for (int i = 0; i < args.length; i++) {
			argTypes[i] = args[i].getClass();
		}
		return invoke(object, methodName, argTypes, args);
	}

	private static Object invoke(Object object, String methodName, Class<?>[] argTypes, Object[] args) {
		try {
			Method method = object.getClass().getDeclaredMethod(methodName, argTypes);
			method.setAccessible(true);
			return method.invoke(object, args);
		} catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
			throw new RuntimeException("Incompatible KafkaProducer version", e);
		}
	}

	/**
	 * Gets and returns the field {@code fieldName} from the given Object {@code object} using
	 * reflection.
	 */
	protected static Object getField(Object object, String fieldName) {
		return getField(object, object.getClass(), fieldName);
	}

	/**
	 * Gets and returns the field {@code fieldName} from the given Object {@code object} using
	 * reflection.
	 */
	private static Object getField(Object object, Class<?> clazz, String fieldName) {
		try {
			Field field = clazz.getDeclaredField(fieldName);
			field.setAccessible(true);
			return field.get(object);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException("Incompatible KafkaProducer version", e);
		}
	}

	/**
	 * Sets the field {@code fieldName} on the given Object {@code object} to {@code value} using
	 * reflection.
	 */
	protected static void setField(Object object, String fieldName, Object value) {
		try {
			Field field = object.getClass().getDeclaredField(fieldName);
			field.setAccessible(true);
			field.set(object, value);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException("Incompatible KafkaProducer version", e);
		}
	}

}
