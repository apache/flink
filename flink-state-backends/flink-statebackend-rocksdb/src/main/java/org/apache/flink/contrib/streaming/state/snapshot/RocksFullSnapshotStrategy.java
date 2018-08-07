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

package org.apache.flink.contrib.streaming.state.snapshot;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.iterator.RocksStatesPerKeyGroupMergeIterator;
import org.apache.flink.contrib.streaming.state.iterator.RocksTransformingIteratorWrapper;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.function.SupplierWithException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.hasMetaDataFollowsFlag;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.setMetaDataFollowsFlagInKey;

/**
 * Snapshot strategy to create full snapshots of
 * {@link org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend}. Iterates and writes all states from a
 * RocksDB snapshot of the column families.
 *
 * @param <K> type of the backend keys.
 */
public class RocksFullSnapshotStrategy<K> extends SnapshotStrategyBase<K> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksFullSnapshotStrategy.class);

	/** This decorator is used to apply compression per key-group for the written snapshot data. */
	@Nonnull
	private final StreamCompressionDecorator keyGroupCompressionDecorator;

	public RocksFullSnapshotStrategy(
		@Nonnull RocksDB db,
		@Nonnull ResourceGuard rocksDBResourceGuard,
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnull LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase>> kvStateInformation,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int keyGroupPrefixBytes,
		@Nonnull LocalRecoveryConfig localRecoveryConfig,
		@Nonnull CloseableRegistry cancelStreamRegistry,
		@Nonnull StreamCompressionDecorator keyGroupCompressionDecorator) {
		super(
			db,
			rocksDBResourceGuard,
			keySerializer,
			kvStateInformation,
			keyGroupRange,
			keyGroupPrefixBytes,
			localRecoveryConfig,
			cancelStreamRegistry);

		this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
	}

	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> performSnapshot(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory primaryStreamFactory,
		CheckpointOptions checkpointOptions) throws Exception {

		long startTime = System.currentTimeMillis();

		if (kvStateInformation.isEmpty()) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Asynchronous RocksDB snapshot performed on empty keyed state at {}. Returning null.",
					timestamp);
			}

			return DoneFuture.of(SnapshotResult.empty());
		}

		final SupplierWithException<CheckpointStreamWithResultProvider, Exception> supplier =

			localRecoveryConfig.isLocalRecoveryEnabled() &&
				(CheckpointType.SAVEPOINT != checkpointOptions.getCheckpointType()) ?

				() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
					checkpointId,
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory,
					localRecoveryConfig.getLocalStateDirectoryProvider()) :

				() -> CheckpointStreamWithResultProvider.createSimpleStream(
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory);

		final CloseableRegistry snapshotCloseableRegistry = new CloseableRegistry();

		final RocksDBFullSnapshotCallable snapshotOperation =
			new RocksDBFullSnapshotCallable(supplier, snapshotCloseableRegistry);

		return new SnapshotTask(snapshotOperation);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		// nothing to do.
	}

	/**
	 * Wrapping task to run a {@link RocksDBFullSnapshotCallable} and delegate cancellation.
	 */
	private class SnapshotTask extends FutureTask<SnapshotResult<KeyedStateHandle>> {

		/** Reference to the callable for cancellation. */
		@Nonnull
		private final AutoCloseable callableClose;

		SnapshotTask(@Nonnull RocksDBFullSnapshotCallable callable) {
			super(callable);
			this.callableClose = callable;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			IOUtils.closeQuietly(callableClose);
			return super.cancel(mayInterruptIfRunning);
		}
	}

	/**
	 * Encapsulates the process to perform a full snapshot of a RocksDBKeyedStateBackend.
	 */
	@VisibleForTesting
	private class RocksDBFullSnapshotCallable implements Callable<SnapshotResult<KeyedStateHandle>>, AutoCloseable {

		@Nonnull
		private final KeyGroupRangeOffsets keyGroupRangeOffsets;

		@Nonnull
		private final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier;

		@Nonnull
		private final CloseableRegistry snapshotCloseableRegistry;

		@Nonnull
		private final ResourceGuard.Lease dbLease;

		@Nonnull
		private final Snapshot snapshot;

		@Nonnull
		private final ReadOptions readOptions;

		/**
		 * The state meta data.
		 */
		@Nonnull
		private List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

		/**
		 * The copied column handle.
		 */
		@Nonnull
		private List<Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase>> metaDataCopy;

		private final AtomicBoolean ownedForCleanup;

		RocksDBFullSnapshotCallable(
			@Nonnull SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier,
			@Nonnull CloseableRegistry registry) throws IOException {

			this.ownedForCleanup = new AtomicBoolean(false);
			this.checkpointStreamSupplier = checkpointStreamSupplier;
			this.keyGroupRangeOffsets = new KeyGroupRangeOffsets(keyGroupRange);
			this.snapshotCloseableRegistry = registry;

			this.stateMetaInfoSnapshots = new ArrayList<>(kvStateInformation.size());
			this.metaDataCopy = new ArrayList<>(kvStateInformation.size());
			for (Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase> tuple2 : kvStateInformation.values()) {
				// snapshot meta info
				this.stateMetaInfoSnapshots.add(tuple2.f1.snapshot());
				this.metaDataCopy.add(tuple2);
			}

			this.dbLease = rocksDBResourceGuard.acquireResource();

			this.readOptions = new ReadOptions();
			this.snapshot = db.getSnapshot();
			this.readOptions.setSnapshot(snapshot);
		}

		@Override
		public SnapshotResult<KeyedStateHandle> call() throws Exception {

			if (!ownedForCleanup.compareAndSet(false, true)) {
				throw new CancellationException("Snapshot task was already cancelled, stopping execution.");
			}

			final long startTime = System.currentTimeMillis();
			final List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators = new ArrayList<>(metaDataCopy.size());

			try {

				cancelStreamRegistry.registerCloseable(snapshotCloseableRegistry);

				final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider = checkpointStreamSupplier.get();
				snapshotCloseableRegistry.registerCloseable(checkpointStreamWithResultProvider);

				final DataOutputView outputView =
					new DataOutputViewStreamWrapper(checkpointStreamWithResultProvider.getCheckpointOutputStream());

				writeKVStateMetaData(kvStateIterators, outputView);
				writeKVStateData(kvStateIterators, checkpointStreamWithResultProvider);

				final SnapshotResult<KeyedStateHandle> snapshotResult =
					createStateHandlesFromStreamProvider(checkpointStreamWithResultProvider);

				LOG.info("Asynchronous RocksDB snapshot ({}, asynchronous part) in thread {} took {} ms.",
					checkpointStreamSupplier, Thread.currentThread(), (System.currentTimeMillis() - startTime));

				return snapshotResult;

			} finally {

				for (Tuple2<RocksIteratorWrapper, Integer> kvStateIterator : kvStateIterators) {
					IOUtils.closeQuietly(kvStateIterator.f0);
				}

				cleanupSynchronousStepResources();
			}
		}

		private void cleanupSynchronousStepResources() {
			IOUtils.closeQuietly(readOptions);

			db.releaseSnapshot(snapshot);
			IOUtils.closeQuietly(snapshot);

			IOUtils.closeQuietly(dbLease);

			if (cancelStreamRegistry.unregisterCloseable(snapshotCloseableRegistry)) {
				try {
					snapshotCloseableRegistry.close();
				} catch (Exception ex) {
					LOG.warn("Error closing local registry", ex);
				}
			}
		}

		private SnapshotResult<KeyedStateHandle> createStateHandlesFromStreamProvider(
			CheckpointStreamWithResultProvider checkpointStreamWithResultProvider) throws IOException {
			if (snapshotCloseableRegistry.unregisterCloseable(checkpointStreamWithResultProvider)) {
				return CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult(
					checkpointStreamWithResultProvider.closeAndFinalizeCheckpointStreamResult(),
					keyGroupRangeOffsets);
			} else {
				throw new IOException("Snapshot was already closed before completion.");
			}
		}

		private void writeKVStateMetaData(
			final List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators,
			final DataOutputView outputView) throws IOException {

			int kvStateId = 0;

			for (Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase> tuple2 : metaDataCopy) {

				RocksIteratorWrapper rocksIteratorWrapper =
					getRocksIterator(db, tuple2.f0, tuple2.f1, readOptions);

				kvStateIterators.add(Tuple2.of(rocksIteratorWrapper, kvStateId));
				++kvStateId;
			}

			KeyedBackendSerializationProxy<K> serializationProxy =
				new KeyedBackendSerializationProxy<>(
					// TODO: this code assumes that writing a serializer is threadsafe, we should support to
					// get a serialized form already at state registration time in the future
					keySerializer,
					stateMetaInfoSnapshots,
					!Objects.equals(
						UncompressedStreamCompressionDecorator.INSTANCE,
						keyGroupCompressionDecorator));

			serializationProxy.write(outputView);
		}

		private void writeKVStateData(
			final List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators,
			final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider) throws IOException, InterruptedException {

			byte[] previousKey = null;
			byte[] previousValue = null;
			DataOutputView kgOutView = null;
			OutputStream kgOutStream = null;
			CheckpointStreamFactory.CheckpointStateOutputStream checkpointOutputStream =
				checkpointStreamWithResultProvider.getCheckpointOutputStream();

			try {
				// Here we transfer ownership of RocksIterators to the RocksStatesPerKeyGroupMergeIterator
				try (RocksStatesPerKeyGroupMergeIterator mergeIterator = new RocksStatesPerKeyGroupMergeIterator(
					kvStateIterators, keyGroupPrefixBytes)) {

					//preamble: setup with first key-group as our lookahead
					if (mergeIterator.isValid()) {
						//begin first key-group by recording the offset
						keyGroupRangeOffsets.setKeyGroupOffset(
							mergeIterator.keyGroup(),
							checkpointOutputStream.getPos());
						//write the k/v-state id as metadata
						kgOutStream = keyGroupCompressionDecorator.decorateWithCompression(checkpointOutputStream);
						kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
						//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
						kgOutView.writeShort(mergeIterator.kvStateId());
						previousKey = mergeIterator.key();
						previousValue = mergeIterator.value();
						mergeIterator.next();
					}

					//main loop: write k/v pairs ordered by (key-group, kv-state), thereby tracking key-group offsets.
					while (mergeIterator.isValid()) {

						assert (!hasMetaDataFollowsFlag(previousKey));

						//set signal in first key byte that meta data will follow in the stream after this k/v pair
						if (mergeIterator.isNewKeyGroup() || mergeIterator.isNewKeyValueState()) {

							//be cooperative and check for interruption from time to time in the hot loop
							checkInterrupted();

							setMetaDataFollowsFlagInKey(previousKey);
						}

						writeKeyValuePair(previousKey, previousValue, kgOutView);

						//write meta data if we have to
						if (mergeIterator.isNewKeyGroup()) {
							//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
							kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
							// this will just close the outer stream
							kgOutStream.close();
							//begin new key-group
							keyGroupRangeOffsets.setKeyGroupOffset(
								mergeIterator.keyGroup(),
								checkpointOutputStream.getPos());
							//write the kev-state
							//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
							kgOutStream = keyGroupCompressionDecorator.decorateWithCompression(checkpointOutputStream);
							kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
							kgOutView.writeShort(mergeIterator.kvStateId());
						} else if (mergeIterator.isNewKeyValueState()) {
							//write the k/v-state
							//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
							kgOutView.writeShort(mergeIterator.kvStateId());
						}

						//request next k/v pair
						previousKey = mergeIterator.key();
						previousValue = mergeIterator.value();
						mergeIterator.next();
					}
				}

				//epilogue: write last key-group
				if (previousKey != null) {
					assert (!hasMetaDataFollowsFlag(previousKey));
					setMetaDataFollowsFlagInKey(previousKey);
					writeKeyValuePair(previousKey, previousValue, kgOutView);
					//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
					kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
					// this will just close the outer stream
					kgOutStream.close();
					kgOutStream = null;
				}

			} finally {
				// this will just close the outer stream
				IOUtils.closeQuietly(kgOutStream);
			}
		}

		private void writeKeyValuePair(byte[] key, byte[] value, DataOutputView out) throws IOException {
			BytePrimitiveArraySerializer.INSTANCE.serialize(key, out);
			BytePrimitiveArraySerializer.INSTANCE.serialize(value, out);
		}

		private void checkInterrupted() throws InterruptedException {
			if (Thread.currentThread().isInterrupted()) {
				throw new InterruptedException("RocksDB snapshot interrupted.");
			}
		}

		@Override
		public void close() throws Exception {

			if (ownedForCleanup.compareAndSet(false, true)) {
				cleanupSynchronousStepResources();
			}

			if (cancelStreamRegistry.unregisterCloseable(snapshotCloseableRegistry)) {
				snapshotCloseableRegistry.close();
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static RocksIteratorWrapper getRocksIterator(
		RocksDB db,
		ColumnFamilyHandle columnFamilyHandle,
		RegisteredStateMetaInfoBase metaInfo,
		ReadOptions readOptions) {
		StateSnapshotTransformer<byte[]> stateSnapshotTransformer = null;
		if (metaInfo instanceof RegisteredKeyValueStateBackendMetaInfo) {
			stateSnapshotTransformer = (StateSnapshotTransformer<byte[]>)
				((RegisteredKeyValueStateBackendMetaInfo<?, ?>) metaInfo).getSnapshotTransformer();
		}
		RocksIterator rocksIterator = db.newIterator(columnFamilyHandle, readOptions);
		return stateSnapshotTransformer == null ?
			new RocksIteratorWrapper(rocksIterator) :
			new RocksTransformingIteratorWrapper(rocksIterator, stateSnapshotTransformer);
	}
}
