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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pulsar.internal.CachedPulsarClient;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.SourceSinkUtils;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarSinkSemantic;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchema;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializableObject;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Flink Sink to produce data into Pulsar topics.
 *
 * <p>Please note that this producer provides at-least-once reliability guarantees when
 * checkpoints are enabled and setFlushOnCheckpoint(true) is set.
 * Otherwise, the producer doesn't provide any reliability guarantees.
 *
 * @param <T> Type of the messages to write into pulsar.
 */
@PublicEvolving
abstract class FlinkPulsarSinkBase<T> extends TwoPhaseCommitSinkFunction<T, FlinkPulsarSinkBase.PulsarTransactionState<T>, Void> implements CheckpointedFunction {

	private static final Logger log = LoggerFactory.getLogger(FlinkPulsarSinkBase.class);

	protected String adminUrl;

	protected ClientConfigurationData clientConfigurationData;

	protected final Map<String, String> caseInsensitiveParams;

	protected final Map<String, Object> producerConf;

	protected final Properties properties;

	protected boolean flushOnCheckpoint;

	protected boolean failOnWrite;

	protected long transactionTimeout;

	protected long maxBlockTimeMs;

	protected int sendTimeOutMs;

	/**
	 * Lock for accessing the pending records.
	 */
	protected final SerializableObject pendingRecordsLock = new SerializableObject();

	/**
	 * Number of unacknowledged records.
	 */
	protected long pendingRecords = 0L;

	protected ConcurrentHashMap<TxnID, List<MessageId>> tid2MessagesMap;

	protected ConcurrentHashMap<TxnID, List<CompletableFuture<MessageId>>> tid2FuturesMap;

	protected final boolean forcedTopic;

	protected final String defaultTopic;

	protected final PulsarSerializationSchema<T> serializationSchema;

	protected final MessageRouter messageRouter;

	protected transient volatile Throwable failedWrite;

	protected transient PulsarAdmin admin;

	protected transient BiConsumer<MessageId, Throwable> sendCallback;

	/**
	 * Semantic chosen for this instance.
	 */
	protected PulsarSinkSemantic semantic;

	protected transient Producer<T> singleProducer;

	protected transient Map<String, Producer<T>> topic2Producer;

	public FlinkPulsarSinkBase(
		String adminUrl,
		Optional<String> defaultTopicName,
		ClientConfigurationData clientConf,
		Properties properties,
		PulsarSerializationSchema<T> serializationSchema,
		MessageRouter messageRouter) {
		this(
			adminUrl,
			defaultTopicName,
			clientConf,
			properties,
			serializationSchema,
			messageRouter,
			PulsarSinkSemantic.AT_LEAST_ONCE);
	}

	public FlinkPulsarSinkBase(
		String adminUrl,
		Optional<String> defaultTopicName,
		ClientConfigurationData clientConf,
		Properties properties,
		PulsarSerializationSchema<T> serializationSchema,
		MessageRouter messageRouter,
		PulsarSinkSemantic semantic) {
		super(new TransactionStateSerializer(), VoidSerializer.INSTANCE);

		this.adminUrl = checkNotNull(adminUrl);
		this.semantic = semantic;

		if (defaultTopicName.isPresent()) {
			this.forcedTopic = true;
			this.defaultTopic = defaultTopicName.get();
		} else {
			this.forcedTopic = false;
			this.defaultTopic = null;
		}

		this.serializationSchema = serializationSchema;

		this.messageRouter = messageRouter;

		this.clientConfigurationData = clientConf;

		this.properties = checkNotNull(properties);

		this.caseInsensitiveParams =
			SourceSinkUtils.toCaceInsensitiveParams(Maps.fromProperties(properties));

		this.producerConf =
			SourceSinkUtils.getProducerParams(Maps.fromProperties(properties));

		this.flushOnCheckpoint =
			SourceSinkUtils.flushOnCheckpoint(caseInsensitiveParams);

		this.failOnWrite =
			SourceSinkUtils.failOnWrite(caseInsensitiveParams);

		this.transactionTimeout =
			SourceSinkUtils.getTransactionTimeout(caseInsensitiveParams);

		this.maxBlockTimeMs =
			SourceSinkUtils.getMaxBlockTimeMs(caseInsensitiveParams);

		this.sendTimeOutMs =
			SourceSinkUtils.getSendTimeoutMs(caseInsensitiveParams);

		CachedPulsarClient.setCacheSize(SourceSinkUtils.getClientCacheSize(caseInsensitiveParams));
		if (semantic == PulsarSinkSemantic.EXACTLY_ONCE) {
			// in transactional mode, must set producer sendTimeout to 0;
			this.sendTimeOutMs = 0;
			this.tid2MessagesMap = new ConcurrentHashMap<>();
			this.tid2FuturesMap = new ConcurrentHashMap<>();
			clientConfigurationData.setEnableTransaction(true);
		}
		if (this.clientConfigurationData.getServiceUrl() == null) {
			throw new IllegalArgumentException(
				"ServiceUrl must be supplied in the client configuration");
		}
	}

	public FlinkPulsarSinkBase(
		String serviceUrl,
		String adminUrl,
		Optional<String> defaultTopicName,
		Properties properties,
		PulsarSerializationSchema serializationSchema,
		MessageRouter messageRouter) {
		this(
			adminUrl,
			defaultTopicName,
			PulsarClientUtils.newClientConf(checkNotNull(serviceUrl), properties),
			properties,
			serializationSchema,
			messageRouter);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		checkErroneous();
		super.snapshotState(context);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		if (semantic != PulsarSinkSemantic.NONE
			&& !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
			log.warn(
				"Using {} semantic, but checkpointing is not enabled. Switching to {} semantic.",
				semantic,
				PulsarSinkSemantic.NONE);
			semantic = PulsarSinkSemantic.NONE;
		}

		super.initializeState(context);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		if (flushOnCheckpoint
			&& !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
			log.warn(
				"Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
			flushOnCheckpoint = false;
		}

		admin = PulsarClientUtils.newAdminFromConf(adminUrl, clientConfigurationData);

		serializationSchema.open(
			RuntimeContextInitializationContextAdapters.serializationAdapter(
				getRuntimeContext(),
				metricGroup -> metricGroup.addGroup("user")
			)
		);

		if (forcedTopic) {
			uploadSchema(defaultTopic);
			singleProducer = createProducer(clientConfigurationData, producerConf, defaultTopic,
				serializationSchema.getSchema());
		} else {
			topic2Producer = new HashMap<>();
		}
		//super.open(parameters);
	}

	protected void initializeSendCallback() {
		if (sendCallback != null) {
			return;
		}
		if (failOnWrite) {
			this.sendCallback = (t, u) -> {
				if (failedWrite == null && u == null) {
					acknowledgeMessage();
				} else if (failedWrite == null && u != null) {
					failedWrite = u;
				} else { // failedWrite != null
					log.warn("callback error {}", u);
					// do nothing and wait next checkForError to throw exception
				}
			};
		} else {
			this.sendCallback = (t, u) -> {
				if (failedWrite == null && u != null) {
					log.error(
						"Error while sending message to Pulsar: {}",
						ExceptionUtils.stringifyException(u));
				}
				acknowledgeMessage();
			};
		}
	}

	private void uploadSchema(String topic) {
		SchemaUtils.uploadPulsarSchema(
			admin,
			topic,
			serializationSchema.getSchema().getSchemaInfo());
	}

	@Override
	public void close() throws Exception {
		checkErroneous();
		producerClose();
		checkErroneous();
	}

	protected Producer<T> getProducer(String topic) {
		if (forcedTopic) {
			return singleProducer;
		}

		if (topic2Producer.containsKey(topic)) {
			return topic2Producer.get(topic);
		} else {
			uploadSchema(topic);
			Producer<T> p = createProducer(
				clientConfigurationData,
				producerConf,
				topic,
				serializationSchema.getSchema());
			topic2Producer.put(topic, p);
			return p;
		}
	}

	protected Producer<T> createProducer(
		ClientConfigurationData clientConf,
		Map<String, Object> producerConf,
		String topic,
		Schema<T> schema) {

		try {
			ProducerBuilder<T> builder = CachedPulsarClient
				.getOrCreate(clientConf)
				.newProducer(schema)
				.topic(topic)
				.sendTimeout(sendTimeOutMs, TimeUnit.MILLISECONDS)
				.batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
				// maximizing the throughput
				.batchingMaxMessages(5 * 1024 * 1024)
				.loadConf(producerConf);
			if (messageRouter == null) {
				return builder.create();
			} else {
				return builder.messageRoutingMode(MessageRoutingMode.CustomPartition)
					.messageRouter(messageRouter)
					.create();
			}
		} catch (PulsarClientException e) {
			log.error("Failed to create producer for topic {}", topic);
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			log.error("Failed to getOrCreate a PulsarClient");
			throw new RuntimeException(e);
		}
	}

	public void producerFlush(PulsarTransactionState<T> transaction) throws Exception {
		if (singleProducer != null) {
			singleProducer.flush();
		} else {
			if (topic2Producer != null) {
				for (Producer<?> p : topic2Producer.values()) {
					p.flush();
				}
			}
		}

		if (transaction.isTransactional()) {
			// we check the future was completed and add the messageId to list for persistence.
			List<CompletableFuture<MessageId>> futureList = tid2FuturesMap.get(transaction.transactionalId);
			for (CompletableFuture<MessageId> future : futureList) {
				try {
					MessageId messageId = future.get();
					TxnID transactionalId = transaction.transactionalId;
					tid2MessagesMap
						.computeIfAbsent(transactionalId, key -> new ArrayList<>())
						.add(messageId);
					log.debug(
						"transaction {} add the message {} to messageIdLIst",
						transactionalId,
						messageId);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				} catch (ExecutionException e) {
					throw new RuntimeException(e);
				}
			}
		}

		synchronized (pendingRecordsLock) {
			while (pendingRecords > 0) {
				try {
					pendingRecordsLock.wait();
				} catch (InterruptedException e) {
					// this can be interrupted when the Task has been cancelled.
					// by throwing an exception, we ensure that this checkpoint doesn't get confirmed
					throw new RuntimeException("Flushing got interrupted while checkpointing", e);
				}
			}
		}

		// if the flushed requests has errors, we should propagate it also and fail the checkpoint
		checkErroneous();
	}

	/**
	 * For each checkpoint we create new {@link org.apache.pulsar.client.api.transaction.Transaction} so that new transactions will not clash
	 * with transactions created during previous checkpoints.
	 */
	private Transaction createTransaction() throws Exception {
		PulsarClientImpl client = CachedPulsarClient.getOrCreate(clientConfigurationData);
		Thread.sleep(100);
		Transaction transaction = client
			.newTransaction()
			.withTransactionTimeout(transactionTimeout, TimeUnit.MILLISECONDS)
			.build()
			.get();
		return transaction;
	}

	@Override
	protected PulsarTransactionState<T> beginTransaction() throws Exception {
		switch (semantic) {
			case EXACTLY_ONCE:
				log.debug("transaction is begining in EXACTLY_ONCE mode");
				Transaction transaction = createTransaction();
				long txnIdLeastBits = ((TransactionImpl) transaction).getTxnIdLeastBits();
				long txnIdMostBits = ((TransactionImpl) transaction).getTxnIdMostBits();
				TxnID txnID = new TxnID(txnIdMostBits, txnIdLeastBits);
				tid2MessagesMap.computeIfAbsent(txnID, key -> new ArrayList<>());
				tid2FuturesMap.computeIfAbsent(txnID, key -> new ArrayList<>());
				return new PulsarTransactionState<T>(
					new TxnID(txnIdMostBits, txnIdLeastBits),
					transaction,
					tid2MessagesMap.get(txnID));
			case AT_LEAST_ONCE:
			case NONE:
				// Do not create new producer on each beginTransaction() if it is not necessary
				final PulsarTransactionState<T> currentTransaction = currentTransaction();
				if (currentTransaction != null && currentTransaction.transactionalId != null) {
					return new PulsarTransactionState<T>(
						currentTransaction.transactionalId,
						currentTransaction.getTransaction(),
						currentTransaction.getPendingMessages());
				}
				return new PulsarTransactionState<T>(null, null, new ArrayList<>());
			default:
				throw new UnsupportedOperationException("Not implemented semantic");
		}
	}

	@Override
	protected void preCommit(PulsarTransactionState<T> transaction) throws Exception {
		switch (semantic) {
			case EXACTLY_ONCE:
			case AT_LEAST_ONCE:
				producerFlush(transaction);
				break;
			case NONE:
				break;
			default:
				throw new UnsupportedOperationException("Not implemented semantic");
		}
		if (transaction.isTransactional()) {
			log.debug(
				"{} preCommit with pending message size {}",
				transaction.transactionalId,
				tid2MessagesMap.get(currentTransaction().transactionalId).size());
		} else {
			log.debug("in AT_LEAST_ONCE mode, producer was flushed by preCommit");
		}
		checkErroneous();
	}

	@Override
	protected void commit(PulsarTransactionState<T> transactionState) {
		if (transactionState.isTransactional()) {
			log.debug("transaction {} is committing", transactionState.transactionalId.toString());
			CompletableFuture<Void> future = transactionState.transaction.commit();
			try {
				future.get(maxBlockTimeMs, TimeUnit.MILLISECONDS);
				log.debug(
					"transaction {} is committed with messageID size {}",
					transactionState.transactionalId.toString(),
					tid2MessagesMap.get(transactionState.transactionalId).size());
				tid2MessagesMap.remove(transactionState.transactionalId);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	protected void abort(PulsarTransactionState<T> transactionState) {
		if (transactionState.isTransactional()) {
			CompletableFuture<Void> future = transactionState.transaction.abort();
			log.debug("transaction {} is aborting", transactionState.transactionalId.toString());
			try {
				future.get(maxBlockTimeMs, TimeUnit.MILLISECONDS);
				log.debug("transaction {} is aborted", transactionState.transactionalId.toString());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	protected void recoverAndCommit(PulsarTransactionState<T> transaction) {
		if (transaction.isTransactional()) {
			try {
				log.debug("transaction {} is recoverAndCommit...", transaction.transactionalId);
				TransactionCoordinatorClientImpl tcClient = CachedPulsarClient
					.getOrCreate(clientConfigurationData)
					.getTcClient();
				TxnID transactionalId = transaction.transactionalId;
				tcClient.commit(transactionalId, transaction.pendingMessages);
			} catch (ExecutionException executionException) {
				log.error("Failed to getOrCreate a PulsarClient");
				throw new RuntimeException(executionException);
			} catch (TransactionCoordinatorClientException.InvalidTxnStatusException statusException) {
				// In some cases, the transaction has been committed or aborted before the recovery,
				// but Flink has not yet sensed it. When flink recover this job, it will commit or
				// abort the transaction again, then Pulsar will throw a duplicate operation error,
				// we catch the error without doing anything to deal with it
				log.debug("transaction {} is already committed...", transaction.transactionalId);
			} catch (TransactionCoordinatorClientException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	protected void recoverAndAbort(PulsarTransactionState<T> transaction) {
		if (transaction.isTransactional()) {
			try {
				log.debug("transaction {} is recoverAndAbort...", transaction.transactionalId);
				TransactionCoordinatorClientImpl tcClient = CachedPulsarClient
					.getOrCreate(clientConfigurationData)
					.getTcClient();
				TxnID transactionalId = transaction.transactionalId;
				tcClient.abort(transactionalId, transaction.pendingMessages);
			} catch (ExecutionException executionException) {
				log.error("Failed to getOrCreate a PulsarClient");
				throw new RuntimeException(executionException);
			} catch (TransactionCoordinatorClientException.InvalidTxnStatusException statusException) {
				// In some cases, the transaction has been committed or aborted before the recovery,
				// but Flink has not yet sensed it. When flink recover this job, it will commit or
				// abort the transaction again, then Pulsar will throw a duplicate operation error,
				// we catch the error without doing anything to deal with it
				log.debug("transaction {} is already aborted...", transaction.transactionalId);
			} catch (TransactionCoordinatorClientException e) {
				throw new RuntimeException(e);
			}
		}
	}

	protected void producerClose() throws Exception {
		producerFlush(currentTransaction());
		if (admin != null) {
			admin.close();
		}
		if (singleProducer != null) {
			singleProducer.close();
		} else {
			if (topic2Producer != null) {
				for (Producer<?> p : topic2Producer.values()) {
					p.close();
				}
				topic2Producer.clear();
			}
		}
	}

	protected void checkErroneous() throws Exception {
		Throwable e = failedWrite;
		if (e != null) {
			// prevent double throwing
			failedWrite = null;
			throw new Exception("Failed to send data to Pulsar: " + e.getMessage(), e);
		}
	}

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

	/**
	 * State for handling transactions.
	 */
	@VisibleForTesting
	@Internal
	public static class PulsarTransactionState<T> {

		private final transient Transaction transaction;

		private final List<MessageId> pendingMessages;

		@Nullable
		final TxnID transactionalId;

		@VisibleForTesting
		public PulsarTransactionState() {
			this(null, null, new ArrayList<>());
		}

		@VisibleForTesting
		public PulsarTransactionState(
			@Nullable TxnID transactionalId,
			@Nullable Transaction transaction,
			List<MessageId> pendingMessages) {
			this.transactionalId = transactionalId;
			this.transaction = transaction;
			this.pendingMessages = pendingMessages;
		}

		public Transaction getTransaction() {
			return transaction;
		}

		boolean isTransactional() {
			return transactionalId != null;
		}

		public List<MessageId> getPendingMessages() {
			return pendingMessages;
		}

		@Override
		public String toString() {
			if (isTransactional()) {
				return String.format(
					"%s [transactionalId=%s] [pendingMessages=%s]",
					this.getClass().getSimpleName(),
					transactionalId.toString(),
					pendingMessages.size());
			} else {
				return String.format(
					"%s this state is not in transactional mode",
					this.getClass().getSimpleName());
			}
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			PulsarTransactionState<?> that = (PulsarTransactionState<?>) o;

			if (!pendingMessages.equals(that.pendingMessages)) {
				return false;
			}
			return
				transactionalId != null ? transactionalId.equals(that.transactionalId) :
					that.transactionalId == null;
		}

		@Override
		public int hashCode() {
			int result = pendingMessages.hashCode();
			result = 31 * result + (transactionalId != null ? transactionalId.hashCode() : 0);
			return result;
		}
	}

	/**
	 * {@link org.apache.flink.api.common.typeutils.TypeSerializer} for
	 * {@link PulsarTransactionState}.
	 */
	@VisibleForTesting
	@Internal
	public static class TransactionStateSerializer<T> extends TypeSerializerSingleton<PulsarTransactionState<T>> {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public PulsarTransactionState<T> createInstance() {
			return null;
		}

		@Override
		public PulsarTransactionState<T> copy(PulsarTransactionState<T> from) {
			return from;
		}

		@Override
		public PulsarTransactionState<T> copy(
			PulsarTransactionState<T> from,
			PulsarTransactionState<T> reuse) {
			return from;
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(
			PulsarTransactionState<T> record,
			DataOutputView target) throws IOException {
			if (record.transactionalId == null) {
				target.writeBoolean(false);
			} else {
				target.writeBoolean(true);
				target.writeLong(record.transactionalId.getMostSigBits());
				target.writeLong(record.transactionalId.getLeastSigBits());
				int size = record.pendingMessages.size();
				target.writeInt(size);
				for (MessageId messageId : record.pendingMessages) {
					byte[] messageData = messageId.toByteArray();
					target.writeInt(messageData.length);
					target.write(messageData);
				}
			}
		}

		@Override
		public PulsarTransactionState<T> deserialize(DataInputView source) throws IOException {
			TxnID transactionalId = null;
			List<MessageId> pendingMessages = new ArrayList<>();
			if (source.readBoolean()) {
				long mostSigBits = source.readLong();
				long leastSigBits = source.readLong();
				transactionalId = new TxnID(mostSigBits, leastSigBits);
				int size = source.readInt();
				for (int i = 0; i < size; i++) {
					int length = source.readInt();
					byte[] messageData = new byte[length];
					source.read(messageData);
					pendingMessages.add(MessageId.fromByteArray(messageData));
				}
			}
			return new PulsarTransactionState<T>(transactionalId, null, pendingMessages);
		}

		@Override
		public PulsarTransactionState<T> deserialize(
			PulsarTransactionState<T> reuse,
			DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(
			DataInputView source, DataOutputView target) throws IOException {
			boolean hasTransactionalId = source.readBoolean();
			target.writeBoolean(hasTransactionalId);
			if (hasTransactionalId) {
				long mostSigBits = source.readLong();
				long leastSigBits = source.readLong();
				target.writeLong(mostSigBits);
				target.writeLong(leastSigBits);
			}
		}

		// -----------------------------------------------------------------------------------

		@Override
		public TypeSerializerSnapshot<PulsarTransactionState<T>> snapshotConfiguration() {
			return new TransactionStateSerializerSnapshot<T>();
		}

		/**
		 * Serializer configuration snapshot for compatibility and format evolution.
		 */
		@SuppressWarnings("WeakerAccess")
		public static final class TransactionStateSerializerSnapshot<T> extends SimpleTypeSerializerSnapshot<PulsarTransactionState<T>> {

			public TransactionStateSerializerSnapshot() {
				super(TransactionStateSerializer::new);
			}
		}
	}
}
