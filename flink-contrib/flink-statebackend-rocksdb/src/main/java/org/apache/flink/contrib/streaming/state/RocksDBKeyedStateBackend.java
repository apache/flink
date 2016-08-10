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

package org.apache.flink.contrib.streaming.state;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.util.SerializableObject;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * A {@link KeyedStateBackend} that stores its state in {@code RocksDB} and will serialize state to
 * streams provided by a {@link org.apache.flink.runtime.state.CheckpointStreamFactory} upon
 * checkpointing. This state backend can store very large state that exceeds memory and spills
 * to disk.
 */
public class RocksDBKeyedStateBackend<K> extends KeyedStateBackend<K> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBKeyedStateBackend.class);

	/** Operator identifier that is used to uniqueify the RocksDB storage path. */
	private final String operatorIdentifier;

	/** JobID for uniquifying backup paths. */
	private final JobID jobId;

	/** The options from the options factory, cached */
	private final ColumnFamilyOptions columnOptions;

	/** Path where this configured instance stores its data directory */
	private final File instanceBasePath;

	/** Path where this configured instance stores its RocksDB data base */
	private final File instanceRocksDBPath;

	/**
	 * Our RocksDB data base, this is used by the actual subclasses of {@link AbstractRocksDBState}
	 * to store state. The different k/v states that we have don't each have their own RocksDB
	 * instance. They all write to this instance but to their own column family.
	 */
	protected volatile RocksDB db;

	/**
	 * Lock for protecting cleanup of the RocksDB db. We acquire this when doing asynchronous
	 * checkpoints and when disposing the db. Otherwise, the asynchronous snapshot might try
	 * iterating over a disposed db.
	 */
	private final SerializableObject dbCleanupLock = new SerializableObject();

	/**
	 * Information about the k/v states as we create them. This is used to retrieve the
	 * column family that is used for a state and also for sanity checks when restoring.
	 */
	private Map<String, Tuple2<ColumnFamilyHandle, StateDescriptor>> kvStateInformation;

	public RocksDBKeyedStateBackend(
			JobID jobId,
			String operatorIdentifier,
			File instanceBasePath,
			DBOptions dbOptions,
			ColumnFamilyOptions columnFamilyOptions,
			TaskKvStateRegistry kvStateRegistry,
			TypeSerializer<K> keySerializer,
			KeyGroupAssigner<K> keyGroupAssigner,
	        KeyGroupRange keyGroupRange
	) throws Exception {

		super(kvStateRegistry, keySerializer, keyGroupAssigner, keyGroupRange);

		this.operatorIdentifier = operatorIdentifier;
		this.jobId = jobId;
		this.columnOptions = columnFamilyOptions;

		this.instanceBasePath = instanceBasePath;
		this.instanceRocksDBPath = new File(instanceBasePath, "db");

		RocksDB.loadLibrary();

		if (!instanceBasePath.exists()) {
			if (!instanceBasePath.mkdirs()) {
				throw new RuntimeException("Could not create RocksDB data directory.");
			}
		}

		// clean it, this will remove the last part of the path but RocksDB will recreate it
		try {
			if (instanceRocksDBPath.exists()) {
				LOG.warn("Deleting already existing db directory {}.", instanceRocksDBPath);
				FileUtils.deleteDirectory(instanceRocksDBPath);
			}
		} catch (IOException e) {
			throw new RuntimeException("Error cleaning RocksDB data directory.", e);
		}

		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(1);
		// RocksDB seems to need this...
		columnFamilyDescriptors.add(new ColumnFamilyDescriptor("default".getBytes()));
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
		try {
			db = RocksDB.open(dbOptions, instanceRocksDBPath.getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException("Error while opening RocksDB instance.", e);
		}

		kvStateInformation = new HashMap<>();
	}

	@Override
	public void close() throws Exception {
		super.close();

		// we have to lock because we might have an asynchronous checkpoint going on
		synchronized (dbCleanupLock) {
			if (db != null) {
				for (Tuple2<ColumnFamilyHandle, StateDescriptor> column : kvStateInformation.values()) {
					column.f0.dispose();
				}

				db.dispose();
				db = null;
			}
		}

		FileUtils.deleteDirectory(instanceBasePath);
	}

	@Override
	public Future<KeyGroupsStateHandle> snapshot(
			long checkpointId,
			long timestamp,
			CheckpointStreamFactory streamFactory) throws Exception {
		throw new RuntimeException("Not implemented.");
	}

	// ------------------------------------------------------------------------
	//  State factories
	// ------------------------------------------------------------------------

	/**
	 * Creates a column family handle for use with a k/v state. When restoring from a snapshot
	 * we don't restore the individual k/v states, just the global RocksDB data base and the
	 * list of column families. When a k/v state is first requested we check here whether we
	 * already have a column family for that and return it or create a new one if it doesn't exist.
	 *
	 * <p>This also checks whether the {@link StateDescriptor} for a state matches the one
	 * that we checkpointed, i.e. is already in the map of column families.
	 */
	protected ColumnFamilyHandle getColumnFamily(StateDescriptor descriptor) {

		Tuple2<ColumnFamilyHandle, StateDescriptor> stateInfo = kvStateInformation.get(descriptor.getName());

		if (stateInfo != null) {
			if (!stateInfo.f1.equals(descriptor)) {
				throw new RuntimeException("Trying to access state using wrong StateDescriptor, was " + stateInfo.f1 + " trying access with " + descriptor);
			}
			return stateInfo.f0;
		}

		ColumnFamilyDescriptor columnDescriptor = new ColumnFamilyDescriptor(descriptor.getName().getBytes(), columnOptions);

		try {
			ColumnFamilyHandle columnFamily = db.createColumnFamily(columnDescriptor);
			kvStateInformation.put(descriptor.getName(), new Tuple2<>(columnFamily, descriptor));
			return columnFamily;
		} catch (RocksDBException e) {
			throw new RuntimeException("Error creating ColumnFamilyHandle.", e);
		}
	}

	@Override
	protected <N, T> ValueState<T> createValueState(TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc);

		return new RocksDBValueState<>(columnFamily, namespaceSerializer,  stateDesc, this);
	}

	@Override
	protected <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer,
			ListStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc);

		return new RocksDBListState<>(columnFamily, namespaceSerializer, stateDesc, this);
	}

	@Override
	protected <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer,
			ReducingStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc);

		return new RocksDBReducingState<>(columnFamily, namespaceSerializer,  stateDesc, this);
	}

	@Override
	protected <N, T, ACC> FoldingState<T, ACC> createFoldingState(TypeSerializer<N> namespaceSerializer,
			FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc);

		return new RocksDBFoldingState<>(columnFamily, namespaceSerializer, stateDesc, this);
	}
}
