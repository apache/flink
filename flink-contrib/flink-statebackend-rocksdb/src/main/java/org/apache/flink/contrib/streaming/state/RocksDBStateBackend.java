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
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.api.common.state.StateBackend;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.rocksdb.Options;
import org.rocksdb.StringAppendOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateBackend.class);
	
	
	/** The checkpoint directory that we copy the RocksDB backups to. */
	private final Path checkpointDirectory;

	/** The state backend that stores the non-partitioned state */
	private final AbstractStateBackend nonPartitionedStateBackend;

	/** Operator identifier that is used to uniqueify the RocksDB storage path. */
	private String operatorIdentifier;

	/** JobID for uniquifying backup paths. */
	private JobID jobId;

	// DB storage directories
	
	/** Base paths for RocksDB directory, as configured. May be null. */
	private Path[] configuredDbBasePaths;

	/** Base paths for RocksDB directory, as initialized */
	private File[] initializedDbBasePaths;
	
	private int nextDirectory;
	
	// RocksDB options
	
	/** The pre-configured option settings */
	private PredefinedOptions predefinedOptions = PredefinedOptions.DEFAULT;
	
	/** The options factory to create the RocksDB options in the cluster */
	private OptionsFactory optionsFactory;
	
	/** The options from the options factory, cached */
	private transient Options rocksDbOptions;
	
	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@code RocksDBStateBackend} that stores its checkpoint data in the
	 * file system and location defined by the given URI.
	 * 
	 * <p>A state backend that stores checkpoints in HDFS or S3 must specify the file system
	 * host and port in the URI, or have the Hadoop configuration that describes the file system
	 * (host / high-availability group / possibly credentials) either referenced from the Flink
	 * config, or included in the classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem and path to the checkpoint data directory.
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public RocksDBStateBackend(String checkpointDataUri) throws IOException {
		this(new Path(checkpointDataUri).toUri());
	}

	/**
	 * Creates a new {@code RocksDBStateBackend} that stores its checkpoint data in the
	 * file system and location defined by the given URI.
	 *
	 * <p>A state backend that stores checkpoints in HDFS or S3 must specify the file system
	 * host and port in the URI, or have the Hadoop configuration that describes the file system
	 * (host / high-availability group / possibly credentials) either referenced from the Flink
	 * config, or included in the classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem and path to the checkpoint data directory.
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public RocksDBStateBackend(URI checkpointDataUri) throws IOException {
		// creating the FsStateBackend automatically sanity checks the URI
		FsStateBackend fsStateBackend = new FsStateBackend(checkpointDataUri);
		
		this.nonPartitionedStateBackend = fsStateBackend;
		this.checkpointDirectory = fsStateBackend.getBasePath();
	}


	public RocksDBStateBackend(
			String checkpointDataUri, AbstractStateBackend nonPartitionedStateBackend) throws IOException {
		
		this(new Path(checkpointDataUri).toUri(), nonPartitionedStateBackend);
	}
	
	public RocksDBStateBackend(
			URI checkpointDataUri, AbstractStateBackend nonPartitionedStateBackend) throws IOException {

		this.nonPartitionedStateBackend = requireNonNull(nonPartitionedStateBackend);
		this.checkpointDirectory = FsStateBackend.validateAndNormalizeUri(checkpointDataUri);
	}

	// ------------------------------------------------------------------------
	//  State backend methods
	// ------------------------------------------------------------------------
	
	@Override
	public void initializeForJob(
			Environment env, 
			String operatorIdentifier,
			TypeSerializer<?> keySerializer) throws Exception {
		
		super.initializeForJob(env, operatorIdentifier, keySerializer);

		this.nonPartitionedStateBackend.initializeForJob(env, operatorIdentifier, keySerializer);
		
		this.operatorIdentifier = operatorIdentifier.replace(" ", "");
		this.jobId = env.getJobID();
		
		// initialize the paths where the local RocksDB files should be stored
		if (configuredDbBasePaths == null) {
			// initialize from the temp directories
			initializedDbBasePaths = env.getIOManager().getSpillingDirectories();
		}
		else {
			List<File> dirs = new ArrayList<>(configuredDbBasePaths.length);
			String errorMessage = "";
			
			for (Path path : configuredDbBasePaths) {
				File f = new File(path.toUri().getPath());
				File testDir = new File(f, UUID.randomUUID().toString());
				if (!testDir.mkdirs()) {
					String msg = "Local DB files directory '" + path
							+ "' does not exist and cannot be created. ";
					LOG.error(msg);
					errorMessage += msg;
				} else {
					dirs.add(f);
				}
			}
			
			if (dirs.isEmpty()) {
				throw new Exception("No local storage directories available. " + errorMessage);
			} else {
				initializedDbBasePaths = dirs.toArray(new File[dirs.size()]);
			}
		}
		
		nextDirectory = new Random().nextInt(initializedDbBasePaths.length);
	}

	@Override
	public void disposeAllStateForCurrentJob() throws Exception {
		nonPartitionedStateBackend.disposeAllStateForCurrentJob();
	}

	@Override
	public void close() throws Exception {
		nonPartitionedStateBackend.close();
		
		Options opt = this.rocksDbOptions;
		if (opt != null) {
			opt.dispose();
			this.rocksDbOptions = null;
		}
	}

	File getDbPath(String stateName) {
		return new File(new File(new File(getNextStoragePath(), jobId.toString()), operatorIdentifier), stateName);
	}

	String getCheckpointPath(String stateName) {
		return checkpointDirectory + "/" + jobId.toString() + "/" + operatorIdentifier + "/" + stateName;
	}
	
	File[] getStoragePaths() {
		return initializedDbBasePaths;
	}
	
	File getNextStoragePath() {
		int ni = nextDirectory + 1;
		ni = ni >= initializedDbBasePaths.length ? 0 : ni;
		nextDirectory = ni;
		
		return initializedDbBasePaths[ni];
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
	protected <N, T, ACC> FoldingState<T, ACC> createFoldingState(TypeSerializer<N> namespaceSerializer,
			FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {

		File dbPath = getDbPath(stateDesc.getName());
		String checkpointPath = getCheckpointPath(stateDesc.getName());
		return new RocksDBFoldingState<>(keySerializer, namespaceSerializer,
				stateDesc, dbPath, checkpointPath, getRocksDBOptions());
	}

	@Override
	public CheckpointStateOutputStream createCheckpointStateOutputStream(
			long checkpointID, long timestamp) throws Exception {
		
		return nonPartitionedStateBackend.createCheckpointStateOutputStream(checkpointID, timestamp);
	}

	@Override
	public <S extends Serializable> StateHandle<S> checkpointStateSerializable(
			S state, long checkpointID, long timestamp) throws Exception {
		
		return nonPartitionedStateBackend.checkpointStateSerializable(state, checkpointID, timestamp);
	}

	// ------------------------------------------------------------------------
	//  Parameters
	// ------------------------------------------------------------------------

	/**
	 * Sets the path where the RocksDB local database files should be stored on the local
	 * file system. Setting this path overrides the default behavior, where the
	 * files are stored across the configured temp directories.
	 * 
	 * <p>Passing {@code null} to this function restores the default behavior, where the configured
	 * temp directories will be used.
	 * 
	 * @param path The path where the local RocksDB database files are stored.
	 */
	public void setDbStoragePath(String path) {
		setDbStoragePaths(path == null ? null : new String[] { path });
	}

	/**
	 * Sets the paths across which the local RocksDB database files are distributed on the local
	 * file system. Setting these paths overrides the default behavior, where the
	 * files are stored across the configured temp directories.
	 * 
	 * <p>Each distinct state will be stored in one path, but when the state backend creates
	 * multiple states, they will store their files on different paths.
	 * 
	 * <p>Passing {@code null} to this function restores the default behavior, where the configured
	 * temp directories will be used.
	 * 
	 * @param paths The paths across which the local RocksDB database files will be spread. 
	 */
	public void setDbStoragePaths(String... paths) {
		if (paths == null) {
			configuredDbBasePaths = null;
		} 
		else if (paths.length == 0) {
			throw new IllegalArgumentException("empty paths");
		}
		else {
			Path[] pp = new Path[paths.length];
			
			for (int i = 0; i < paths.length; i++) {
				if (paths[i] == null) {
					throw new IllegalArgumentException("null path");
				}
				
				pp[i] = new Path(paths[i]);
				String scheme = pp[i].toUri().getScheme();
				if (scheme != null && !scheme.equalsIgnoreCase("file")) {
					throw new IllegalArgumentException("Path " + paths[i] + " has a non local scheme");
				}
			}
			
			configuredDbBasePaths = pp;
		}
	}

	/**
	 * 
	 * @return The configured DB storage paths, or null, if none were configured. 
	 */
	public String[] getDbStoragePaths() {
		if (configuredDbBasePaths == null) {
			return null;
		} else {
			String[] paths = new String[configuredDbBasePaths.length];
			for (int i = 0; i < paths.length; i++) {
				paths[i] = configuredDbBasePaths[i].toString();
			}
			return paths;
		}
	}
	
	// ------------------------------------------------------------------------
	//  Parametrize with RocksDB Options
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
