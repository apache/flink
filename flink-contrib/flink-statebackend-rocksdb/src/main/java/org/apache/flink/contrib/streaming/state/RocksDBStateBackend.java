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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.StateBackend;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

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

	// ------------------------------------------------------------------------
	//  Static configuration values
	// ------------------------------------------------------------------------

	/** The state backend that we use for creating checkpoint streams. */
	private final AbstractStateBackend checkpointStreamBackend;

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
	private transient DBOptions dbOptions;
	private transient ColumnFamilyOptions columnOptions;

	/** Whether we already lazily initialized our local storage directories. */
	private transient boolean isInitialized = false;


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

		this.checkpointStreamBackend = fsStateBackend;
	}


	public RocksDBStateBackend(String checkpointDataUri, AbstractStateBackend nonPartitionedStateBackend) throws IOException {
		this(new Path(checkpointDataUri).toUri(), nonPartitionedStateBackend);
	}

	public RocksDBStateBackend(URI checkpointDataUri, AbstractStateBackend checkpointStreamBackend) throws IOException {
		this.checkpointStreamBackend = requireNonNull(checkpointStreamBackend);
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		isInitialized = false;
	}
	// ------------------------------------------------------------------------
	//  State backend methods
	// ------------------------------------------------------------------------

	private void lazyInitializeForJob(
			Environment env,
			String operatorIdentifier) throws Exception {

		if (isInitialized) {
			return;
		}

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
				testDir.delete();
			}

			if (dirs.isEmpty()) {
				throw new Exception("No local storage directories available. " + errorMessage);
			} else {
				initializedDbBasePaths = dirs.toArray(new File[dirs.size()]);
			}
		}

		nextDirectory = new Random().nextInt(initializedDbBasePaths.length);

		isInitialized = true;
	}

	private File getDbPath() {
		return new File(new File(getNextStoragePath(), jobId.toString()), operatorIdentifier);
	}

	private File getNextStoragePath() {
		int ni = nextDirectory + 1;
		ni = ni >= initializedDbBasePaths.length ? 0 : ni;
		nextDirectory = ni;

		return initializedDbBasePaths[ni];
	}

	@Override
	public CheckpointStreamFactory createStreamFactory(JobID jobId,
			String operatorIdentifier) throws IOException {
		return checkpointStreamBackend.createStreamFactory(jobId, operatorIdentifier);
	}

	@Override
	public <K> KeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry) throws Exception {

		lazyInitializeForJob(env, operatorIdentifier);

		File instanceBasePath = new File(getDbPath(), UUID.randomUUID().toString());

		return new RocksDBKeyedStateBackend<>(
				jobID,
				operatorIdentifier,
				instanceBasePath,
				getDbOptions(),
				getColumnOptions(),
				kvStateRegistry,
				keySerializer,
				numberOfKeyGroups,
				keyGroupRange);
	}

	@Override
	public <K> KeyedStateBackend<K> restoreKeyedStateBackend(Environment env, JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			List<KeyGroupsStateHandle> restoredState,
			TaskKvStateRegistry kvStateRegistry) throws Exception {

		lazyInitializeForJob(env, operatorIdentifier);

		File instanceBasePath = new File(getDbPath(), UUID.randomUUID().toString());
		return new RocksDBKeyedStateBackend<>(
				jobID,
				operatorIdentifier,
				instanceBasePath,
				getDbOptions(),
				getColumnOptions(),
				kvStateRegistry,
				keySerializer,
				numberOfKeyGroups,
				keyGroupRange,
				restoredState);
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
	 * Gets the RocksDB {@link DBOptions} to be used for all RocksDB instances.
	 */
	public DBOptions getDbOptions() {
		if (dbOptions == null) {
			// initial options from pre-defined profile
			DBOptions opt = predefinedOptions.createDBOptions();

			// add user-defined options, if specified
			if (optionsFactory != null) {
				opt = optionsFactory.createDBOptions(opt);
			}

			// add necessary default options
			opt = opt.setCreateIfMissing(true);

			dbOptions = opt;
		}
		return dbOptions;
	}

	/**
	 * Gets the RocksDB {@link ColumnFamilyOptions} to be used for all RocksDB instances.
	 */
	public ColumnFamilyOptions getColumnOptions() {
		if (columnOptions == null) {
			// initial options from pre-defined profile
			ColumnFamilyOptions opt = predefinedOptions.createColumnOptions();

			// add user-defined options, if specified
			if (optionsFactory != null) {
				opt = optionsFactory.createColumnOptions(opt);
			}

			columnOptions = opt;
		}
		return columnOptions;
	}
}
