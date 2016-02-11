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

import java.io.File;
import java.io.Serializable;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.api.common.state.StateBackend;

import org.rocksdb.Options;
import org.rocksdb.StringAppendOperator;

import static java.util.Objects.requireNonNull;

/**
 * A {@link StateBackend} that stores its state in {@code RocksDB}. This state backend can
 * store very large state that exceeds memory and spills to disk.
 * 
 * <p>All key/value state (including windows) is stored in the key/value index of RocksDB.
 * For persistence against loss of machines, checkpoints take a snapshot of the
 * RocksDB database, and persist that snapshot in a file system (by default) or
 * another configurable state backend.
 * 
 * <p>The behavior of the RocksDB instances can be parametrized by setting RocksDB Options
 * using the methods {@link #setPredefinedOptions(PredefinedOptions)} and
 * {@link #setOptions(OptionsFactory)}.
 */
public class RocksDBStateBackend extends AbstractStateBackend {
	private static final long serialVersionUID = 1L;

	/** Base path for RocksDB directory. */
	private final String dbBasePath;

	/** The checkpoint directory that we snapshot RocksDB backups to. */
	private final String checkpointDirectory;

	/** Operator identifier that is used to uniqueify the RocksDB storage path. */
	private String operatorIdentifier;

	/** JobID for uniquifying backup paths. */
	private JobID jobId;

	private AbstractStateBackend backingStateBackend;

	/** The pre-configured option settings */
	private PredefinedOptions predefinedOptions = PredefinedOptions.DEFAULT;
	
	/** The options factory to create the RocksDB options in the cluster */
	private OptionsFactory optionsFactory;
	
	/** The options from the options factory, cached */
	private transient Options rocksDbOptions;
	
	// ------------------------------------------------------------------------

	public RocksDBStateBackend(String dbBasePath, String checkpointDirectory, AbstractStateBackend backingStateBackend) {
		this.dbBasePath = requireNonNull(dbBasePath);
		this.checkpointDirectory = requireNonNull(checkpointDirectory);
		this.backingStateBackend = requireNonNull(backingStateBackend);
	}

	// ------------------------------------------------------------------------
	
	@Override
	public void initializeForJob(
			Environment env, 
			String operatorIdentifier,
			TypeSerializer<?> keySerializer) throws Exception
	{
		super.initializeForJob(env, operatorIdentifier, keySerializer);
		this.operatorIdentifier = operatorIdentifier.replace(" ", "");
		backingStateBackend.initializeForJob(env, operatorIdentifier, keySerializer);
		this.jobId = env.getJobID();
	}

	@Override
	public void disposeAllStateForCurrentJob() throws Exception {}

	@Override
	public void close() throws Exception {
		Options opt = this.rocksDbOptions;
		if (opt != null) {
			opt.dispose();
			this.rocksDbOptions = null;
		}
	}

	private File getDbPath(String stateName) {
		return new File(new File(new File(new File(dbBasePath), jobId.toString()), operatorIdentifier), stateName);
	}

	private String getCheckpointPath(String stateName) {
		return checkpointDirectory + "/" + jobId.toString() + "/" + operatorIdentifier + "/" + stateName;
	}

	// ------------------------------------------------------------------------
	//  State factories
	// ------------------------------------------------------------------------
	
	@Override
	protected <N, T> ValueState<T> createValueState(TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<T> stateDesc) throws Exception {
		File dbPath = getDbPath(stateDesc.getName());
		String checkpointPath = getCheckpointPath(stateDesc.getName());
		
		return new RocksDBValueState<>(keySerializer, namespaceSerializer, 
				stateDesc, dbPath, checkpointPath, getRocksDBOptions());
	}

	@Override
	protected <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer,
		ListStateDescriptor<T> stateDesc) throws Exception {
		File dbPath = getDbPath(stateDesc.getName());
		String checkpointPath = getCheckpointPath(stateDesc.getName());
		
		return new RocksDBListState<>(keySerializer, namespaceSerializer, 
				stateDesc, dbPath, checkpointPath, getRocksDBOptions());
	}

	@Override
	protected <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer,
		ReducingStateDescriptor<T> stateDesc) throws Exception {
		File dbPath = getDbPath(stateDesc.getName());
		String checkpointPath = getCheckpointPath(stateDesc.getName());
		
		return new RocksDBReducingState<>(keySerializer, namespaceSerializer, 
				stateDesc, dbPath, checkpointPath, getRocksDBOptions());
	}

	@Override
	public CheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID,
		long timestamp) throws Exception {
		return backingStateBackend.createCheckpointStateOutputStream(checkpointID, timestamp);
	}

	@Override
	public <S extends Serializable> StateHandle<S> checkpointStateSerializable(S state,
		long checkpointID,
		long timestamp) throws Exception {
		return backingStateBackend.checkpointStateSerializable(state, checkpointID, timestamp);
	}
	
	// ------------------------------------------------------------------------
	//  Parametrize with Options
	// ------------------------------------------------------------------------

	/**
	 * Sets the predefined options for RocksDB.
	 * 
	 * <p>If a user-defined options factory is set (via {@link #setOptions(OptionsFactory)}),
	 * then the options from the factory are applied on top of the here specified
	 * predefined options.
	 * 
	 * @param options The options to set (must not be null).
	 */
	public void setPredefinedOptions(PredefinedOptions options) {
		predefinedOptions = requireNonNull(options);
	}

	/**
	 * Gets the currently set predefined options for RocksDB.
	 * The default options (if nothing was set via {@link #setPredefinedOptions(PredefinedOptions)})
	 * are {@link PredefinedOptions#DEFAULT}.
	 * 
	 * <p>If a user-defined  options factory is set (via {@link #setOptions(OptionsFactory)}),
	 * then the options from the factory are applied on top of the predefined options.
	 * 
	 * @return The currently set predefined options for RocksDB.
	 */
	public PredefinedOptions getPredefinedOptions() {
		return predefinedOptions;
	}

	/**
	 * Sets {@link org.rocksdb.Options} for the RocksDB instances.
	 * Because the options are not serializable and hold native code references,
	 * they must be specified through a factory.
	 * 
	 * <p>The options created by the factory here are applied on top of the pre-defined 
	 * options profile selected via {@link #setPredefinedOptions(PredefinedOptions)}.
	 * If the pre-defined options profile is the default
	 * ({@link PredefinedOptions#DEFAULT}), then the factory fully controls the RocksDB
	 * options.
	 * 
	 * @param optionsFactory The options factory that lazily creates the RocksDB options.
	 */
	public void setOptions(OptionsFactory optionsFactory) {
		this.optionsFactory = optionsFactory;
	}

	/**
	 * Gets the options factory that lazily creates the RocksDB options.
	 * 
	 * @return The options factory.
	 */
	public OptionsFactory getOptions() {
		return optionsFactory;
	}

	/**
	 * Gets the RocksDB Options to be used for all RocksDB instances.
	 */
	Options getRocksDBOptions() {
		if (rocksDbOptions == null) {
			// initial options from pre-defined profile
			Options opt = predefinedOptions.createOptions();

			// add user-defined options, if specified
			if (optionsFactory != null) {
				opt = optionsFactory.createOptions(opt);
			}
			
			// add necessary default options
			opt = opt.setCreateIfMissing(true);
			opt = opt.setMergeOperator(new StringAppendOperator());
			
			rocksDbOptions = opt;
		}
		return rocksDbOptions;
	}
}
