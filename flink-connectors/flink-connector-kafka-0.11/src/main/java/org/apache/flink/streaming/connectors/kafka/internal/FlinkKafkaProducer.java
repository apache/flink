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

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper around KafkaProducer that allows to resume transactions in case of node failure, which allows to implement
 * two phase commit algorithm for exactly-once semantic FlinkKafkaProducer.
 *
 * <p>For happy path usage is exactly the same as {@link org.apache.kafka.clients.producer.KafkaProducer}. User is
 * expected to call:
 *
 * <ul>
 *     <li>{@link FlinkKafkaProducer#initTransactions()}</li>
 *     <li>{@link FlinkKafkaProducer#beginTransaction()}</li>
 *     <li>{@link FlinkKafkaProducer#send(org.apache.kafka.clients.producer.ProducerRecord)}</li>
 *     <li>{@link FlinkKafkaProducer#flush()}</li>
 *     <li>{@link FlinkKafkaProducer#commitTransaction()}</li>
 * </ul>
 *
 * <p>To actually implement two phase commit, it must be possible to always commit a transaction after pre-committing
 * it (here, pre-commit is just a {@link FlinkKafkaProducer#flush()}). In case of some failure between
 * {@link FlinkKafkaProducer#flush()} and {@link FlinkKafkaProducer#commitTransaction()} this class allows to resume
 * interrupted transaction and commit if after a restart:
 *
 * <ul>
 *     <li>{@link FlinkKafkaProducer#initTransactions()}</li>
 *     <li>{@link FlinkKafkaProducer#beginTransaction()}</li>
 *     <li>{@link FlinkKafkaProducer#send(org.apache.kafka.clients.producer.ProducerRecord)}</li>
 *     <li>{@link FlinkKafkaProducer#flush()}</li>
 *     <li>{@link FlinkKafkaProducer#getProducerId()}</li>
 *     <li>{@link FlinkKafkaProducer#getEpoch()}</li>
 *     <li>node failure... restore producerId and epoch from state</li>
 *     <li>{@link FlinkKafkaProducer#resumeTransaction(long, short)}</li>
 *     <li>{@link FlinkKafkaProducer#commitTransaction()}</li>
 * </ul>
 *
 * <p>{@link FlinkKafkaProducer#resumeTransaction(long, short)} replaces {@link FlinkKafkaProducer#initTransactions()}
 * as a way to obtain the producerId and epoch counters. It has to be done, because otherwise
 * {@link FlinkKafkaProducer#initTransactions()} would automatically abort all on going transactions.
 *
 * <p>Second way this implementation differs from the reference {@link org.apache.kafka.clients.producer.KafkaProducer}
 * is that this one actually flushes new partitions on {@link FlinkKafkaProducer#flush()} instead of on
 * {@link FlinkKafkaProducer#commitTransaction()}.
 *
 * <p>The last one minor difference is that it allows to obtain the producerId and epoch counters via
 * {@link FlinkKafkaProducer#getProducerId()} and {@link FlinkKafkaProducer#getEpoch()} methods (which are unfortunately
 * private fields).
 *
 * <p>Those changes are compatible with Kafka's 0.11.0 REST API although it clearly was not the intention of the Kafka's
 * API authors to make them possible.
 *
 * <p>Internally this implementation uses {@link org.apache.kafka.clients.producer.KafkaProducer} and implements
 * required changes via Java Reflection API. It might not be the prettiest solution. An alternative would be to
 * re-implement whole Kafka's 0.11 REST API client on our own.
 */
@PublicEvolving
public class FlinkKafkaProducer<K, V> implements Producer<K, V> {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaProducer.class);

	private final KafkaProducer<K, V> kafkaProducer;

	@Nullable
	private final String transactionalId;

	public FlinkKafkaProducer(Properties properties) {
		transactionalId = properties.getProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
		kafkaProducer = new KafkaProducer<>(properties);
	}

	// -------------------------------- Simple proxy method calls --------------------------------

	@Override
	public void initTransactions() {
		kafkaProducer.initTransactions();
	}

	@Override
	public void beginTransaction() throws ProducerFencedException {
		kafkaProducer.beginTransaction();
	}

	@Override
	public void commitTransaction() throws ProducerFencedException {
		kafkaProducer.commitTransaction();
	}

	@Override
	public void abortTransaction() throws ProducerFencedException {
		kafkaProducer.abortTransaction();
	}

	@Override
	public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {
		kafkaProducer.sendOffsetsToTransaction(offsets, consumerGroupId);
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
		return kafkaProducer.partitionsFor(topic);
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return kafkaProducer.metrics();
	}

	@Override
	public void close() {
		kafkaProducer.close();
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		kafkaProducer.close(timeout, unit);
	}

	// -------------------------------- New methods or methods with changed behaviour --------------------------------

	@Override
	public void flush() {
		kafkaProducer.flush();
		if (transactionalId != null) {
			flushNewPartitions();
		}
	}

	/**
	 * Instead of obtaining producerId and epoch from the transaction coordinator, re-use previously obtained ones,
	 * so that we can resume transaction after a restart. Implementation of this method is based on
	 * {@link org.apache.kafka.clients.producer.KafkaProducer#initTransactions}.
	 */
	public void resumeTransaction(long producerId, short epoch) {
		Preconditions.checkState(producerId >= 0 && epoch >= 0, "Incorrect values for producerId {} and epoch {}", producerId, epoch);
		LOG.info("Attempting to resume transaction {} with producerId {} and epoch {}", transactionalId, producerId, epoch);

		Object transactionManager = getValue(kafkaProducer, "transactionManager");
		synchronized (transactionManager) {
			Object sequenceNumbers = getValue(transactionManager, "sequenceNumbers");

			invoke(transactionManager, "transitionTo", getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.INITIALIZING"));
			invoke(sequenceNumbers, "clear");

			Object producerIdAndEpoch = getValue(transactionManager, "producerIdAndEpoch");
			setValue(producerIdAndEpoch, "producerId", producerId);
			setValue(producerIdAndEpoch, "epoch", epoch);

			invoke(transactionManager, "transitionTo", getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.READY"));

			invoke(transactionManager, "transitionTo", getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.IN_TRANSACTION"));
			setValue(transactionManager, "transactionStarted", true);
		}
	}

	public String getTransactionalId() {
		return transactionalId;
	}

	public long getProducerId() {
		Object transactionManager = getValue(kafkaProducer, "transactionManager");
		Object producerIdAndEpoch = getValue(transactionManager, "producerIdAndEpoch");
		return (long) getValue(producerIdAndEpoch, "producerId");
	}

	public short getEpoch() {
		Object transactionManager = getValue(kafkaProducer, "transactionManager");
		Object producerIdAndEpoch = getValue(transactionManager, "producerIdAndEpoch");
		return (short) getValue(producerIdAndEpoch, "epoch");
	}

	@VisibleForTesting
	public int getTransactionCoordinatorId() {
		Object transactionManager = getValue(kafkaProducer, "transactionManager");
		Node node = (Node) invoke(transactionManager, "coordinator", FindCoordinatorRequest.CoordinatorType.TRANSACTION);
		return node.id();
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
		Object sender = getValue(kafkaProducer, "sender");
		invoke(sender, "wakeup");
		result.await();
	}

	private TransactionalRequestResult enqueueNewPartitions() {
		Object transactionManager = getValue(kafkaProducer, "transactionManager");
		synchronized (transactionManager) {
			Object txnRequestHandler = invoke(transactionManager, "addPartitionsToTransactionHandler");
			invoke(transactionManager, "enqueueRequest", new Class[]{txnRequestHandler.getClass().getSuperclass()}, new Object[]{txnRequestHandler});
			TransactionalRequestResult result = (TransactionalRequestResult) getValue(txnRequestHandler, txnRequestHandler.getClass().getSuperclass(), "result");
			return result;
		}
	}

	private static Enum<?> getEnum(String enumFullName) {
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

	private static Object invoke(Object object, String methodName, Object... args) {
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

	private static Object getValue(Object object, String fieldName) {
		return getValue(object, object.getClass(), fieldName);
	}

	private static Object getValue(Object object, Class<?> clazz, String fieldName) {
		try {
			Field field = clazz.getDeclaredField(fieldName);
			field.setAccessible(true);
			return field.get(object);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException("Incompatible KafkaProducer version", e);
		}
	}

	private static void setValue(Object object, String fieldName, Object value) {
		try {
			Field field = object.getClass().getDeclaredField(fieldName);
			field.setAccessible(true);
			field.set(object, value);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException("Incompatible KafkaProducer version", e);
		}
	}
}
