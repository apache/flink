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
import org.apache.flink.runtime.state.AsyncSnapshotCallable;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.RunnableFuture;

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
public class RocksFullSnapshotStrategy<K> extends RocksDBSnapshotStrategyBase<K> {

	private static final String DESCRIPTION = "Asynchronous incremental RocksDB snapshot";

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
			DESCRIPTION,
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

	@Nonnull
	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> doSnapshot(
		long checkpointId,
		long timestamp,
		@Nonnull CheckpointStreamFactory primaryStreamFactory,
		@Nonnull CheckpointOptions checkpointOptions) throws Exception {

		final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier =
			createCheckpointStreamSupplier(checkpointId, primaryStreamFactory, checkpointOptions);

		final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>(kvStateInformation.size());
		final List<Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase>> metaDataCopy =
			new ArrayList<>(kvStateInformation.size());

		for (Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase> tuple2 : kvStateInformation.values()) {
			// snapshot meta info
			stateMetaInfoSnapshots.add(tuple2.f1.snapshot());
			metaDataCopy.add(tuple2);
		}

		final ResourceGuard.Lease lease = rocksDBResourceGuard.acquireResource();
		final Snapshot snapshot = db.getSnapshot();

		final SnapshotAsynchronousPartCallable asyncSnapshotCallable =
			new SnapshotAsynchronousPartCallable(
				checkpointStreamSupplier,
				lease,
				snapshot,
				stateMetaInfoSnapshots,
				metaDataCopy,
				primaryStreamFactory.toString());

		return asyncSnapshotCallable.toAsyncSnapshotFutureTask(cancelStreamRegistry);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		// nothing to do.
	}

	private SupplierWithException<CheckpointStreamWithResultProvider, Exception> createCheckpointStreamSupplier(
		long checkpointId,
		CheckpointStreamFactory primaryStreamFactory,
		CheckpointOptions checkpointOptions) {

		return localRecoveryConfig.isLocalRecoveryEnabled() &&
			(CheckpointType.SAVEPOINT != checkpointOptions.getCheckpointType()) ?

			() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
				checkpointId,
				CheckpointedStateScope.EXCLUSIVE,
				primaryStreamFactory,
				localRecoveryConfig.getLocalStateDirectoryProvider()) :

			() -> CheckpointStreamWithResultProvider.createSimpleStream(
				CheckpointedStateScope.EXCLUSIVE,
				primaryStreamFactory);
	}

	/**
	 * Encapsulates the process to perform a full snapshot of a RocksDBKeyedStateBackend.
	 */
	@VisibleForTesting
	private class SnapshotAsynchronousPartCallable extends AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>> {

		/** Supplier for the stream into which we write the snapshot. */
		@Nonnull
		private final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier;

		/** This lease protects the native RocksDB resources. */
		@Nonnull
		private final ResourceGuard.Lease dbLease;

		/** RocksDB snapshot. */
		@Nonnull
		private final Snapshot snapshot;

		@Nonnull
		private List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

		@Nonnull
		private List<Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase>> metaDataCopy;

		@Nonnull
		private final String logPathString;

		SnapshotAsynchronousPartCallable(
			@Nonnull SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier,
			@Nonnull ResourceGuard.Lease dbLease,
			@Nonnull Snapshot snapshot,
			@Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
			@Nonnull List<Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase>> metaDataCopy,
			@Nonnull String logPathString) {

			this.checkpointStreamSupplier = checkpointStreamSupplier;
			this.dbLease = dbLease;
			this.snapshot = snapshot;
			this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
			this.metaDataCopy = metaDataCopy;
			this.logPathString = logPathString;
		}

		@Override
		protected SnapshotResult<KeyedStateHandle> callInternal() throws Exception {
			final KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(keyGroupRange);
			final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider =
				checkpointStreamSupplier.get();

			registerCloseableForCancellation(checkpointStreamWithResultProvider);
			writeSnapshotToOutputStream(checkpointStreamWithResultProvider, keyGroupRangeOffsets);

			if (unregisterCloseableFromCancellation(checkpointStreamWithResultProvider)) {
				return CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult(
					checkpointStreamWithResultProvider.closeAndFinalizeCheckpointStreamResult(),
					keyGroupRangeOffsets);
			} else {
				throw new IOException("Stream is already unregistered/closed.");
			}
		}

		@Override
		protected void cleanupProvidedResources() {
			db.releaseSnapshot(snapshot);
			IOUtils.closeQuietly(snapshot);
			IOUtils.closeQuietly(dbLease);
		}

		@Override
		protected void logAsyncSnapshotComplete(long startTime) {
			logAsyncCompleted(logPathString, startTime);
		}

		private void writeSnapshotToOutputStream(
			@Nonnull CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
			@Nonnull KeyGroupRangeOffsets keyGroupRangeOffsets) throws IOException, InterruptedException {

			final List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators =
				new ArrayList<>(metaDataCopy.size());
			final DataOutputView outputView =
				new DataOutputViewStreamWrapper(checkpointStreamWithResultProvider.getCheckpointOutputStream());
			final ReadOptions readOptions = new ReadOptions();
			try {
				readOptions.setSnapshot(snapshot);
				writeKVStateMetaData(kvStateIterators, readOptions, outputView);
				writeKVStateData(kvStateIterators, checkpointStreamWithResultProvider, keyGroupRangeOffsets);
			} finally {

				for (Tuple2<RocksIteratorWrapper, Integer> kvStateIterator : kvStateIterators) {
					IOUtils.closeQuietly(kvStateIterator.f0);
				}

				IOUtils.closeQuietly(readOptions);
			}
		}

		private void writeKVStateMetaData(
			final List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators,
			final ReadOptions readOptions,
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
			final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
			final KeyGroupRangeOffsets keyGroupRangeOffsets) throws IOException, InterruptedException {

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
