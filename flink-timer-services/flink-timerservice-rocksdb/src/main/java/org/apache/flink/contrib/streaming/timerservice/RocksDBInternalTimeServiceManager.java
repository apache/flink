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

package org.apache.flink.contrib.streaming.timerservice;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * An implementation of {@link InternalTimeServiceManager} which creates
 * {@link RocksDBInternalTimerService}.
 *
 * @param <K> The type of the keys in the timers.
 * @param <N> The type of the namesapces in the timers.
 */
public class RocksDBInternalTimeServiceManager<K, N> extends InternalTimeServiceManager<K, N> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBInternalTimeServiceManager.class);

	private static final byte[] EVENT_TIME_NAME = "event_time".getBytes(ConfigConstants.DEFAULT_CHARSET);
	private static final byte[] PROCESSING_TIME_NAME = "processing_time".getBytes(ConfigConstants.DEFAULT_CHARSET);

	/** The number of (re)tries for loading the RocksDB JNI library. */
	private static final int ROCKSDB_LIB_LOADING_ATTEMPTS = 3;

	private static boolean rocksDbInitialized = false;

	/** Base paths for RocksDB directory, as configured.
	 * Null if not yet set, in which case the configuration values will be used.
	 * The configuration defaults to the TaskManager's temp directories. */
	@Nullable
	private File[] baseDirectories;

	/** The path for RocksDB directory. */
	private File dbDirectory;

	/** The db to store timers. */
	private RocksDB db;

	/** The handles for the column families in the db. */
	private ColumnFamilyHandle defaultColumnFamily;
	private ColumnFamilyHandle processingTimeColumnFamily;
	private ColumnFamilyHandle eventTimeColumnFamily;

	/** The options for the db. */
	private DBOptions dbOptions;
	private ColumnFamilyOptions columnFamilyOptions;
	private WriteOptions writeOptions;

	@Override
	public void initialize(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			int totalKeyGroups,
			KeyGroupRange localKeyGroupRange,
			TypeSerializer<K> keySerializer,
			KeyContext keyContext,
			ProcessingTimeService processingTimeService) throws Exception {

		super.initialize(env, jobID, operatorIdentifier, totalKeyGroups,
			localKeyGroupRange, keySerializer, keyContext, processingTimeService);

		// replace all characters that are not legal for filenames with underscore
		String fileCompatibleIdentifier = operatorIdentifier.replaceAll("[^a-zA-Z0-9\\-]", "_");

		// first, make sure that the RocksDB JNI library is loaded
		// we do this explicitly here to have better error handling
		String tempDir = environment.getTaskManagerInfo().getTmpDirectories()[0];
		ensureRocksDBIsLoaded(tempDir);

		if (baseDirectories == null) {
			// initialize from the temp directories
			baseDirectories = environment.getIOManager().getSpillingDirectories();
		} else {
			List<File> dirs = new ArrayList<>(baseDirectories.length);
			StringBuilder errorMessage = new StringBuilder();

			for (File f : baseDirectories) {
				File testDir = new File(f, UUID.randomUUID().toString());
				if (!testDir.mkdirs()) {
					String msg = "Local DB files directory '" + f
							+ "' does not exist and cannot be created. ";
					LOG.error(msg);
					errorMessage.append(msg);
				} else {
					dirs.add(f);
				}
				//noinspection ResultOfMethodCallIgnored
				testDir.delete();
			}

			if (dirs.isEmpty()) {
				throw new IOException("No local storage directories available. " + errorMessage);
			} else {
				baseDirectories = dirs.toArray(new File[dirs.size()]);
			}
		}

		Preconditions.checkState(baseDirectories != null && baseDirectories.length > 0);

		int selectedIndex = new Random().nextInt(baseDirectories.length);
		File selectedBaseDirectory = baseDirectories[selectedIndex];
		dbDirectory = new File(selectedBaseDirectory, "job_" + jobID + "_op_" + fileCompatibleIdentifier + "_uuid_" + UUID.randomUUID());

		if (dbDirectory.exists()) {
			LOG.info("Deleting existing db directory {}.", dbDirectory.getCanonicalPath());
			FileUtils.deleteDirectory(dbDirectory);
		}

		dbOptions = new DBOptions()
				.setCreateIfMissing(true)
				.setUseFsync(false)
				.setMaxOpenFiles(-1);

		columnFamilyOptions = new ColumnFamilyOptions()
				.setMergeOperator(new StringAppendOperator())
				.setCompactionStyle(CompactionStyle.UNIVERSAL);

		writeOptions = new WriteOptions()
				.setDisableWAL(true);

		try {
			ColumnFamilyDescriptor defaultColumnDescriptor =
				new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions);
			List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);

			db = RocksDB.open(
					dbOptions,
					dbDirectory.getCanonicalPath(),
					Collections.singletonList(defaultColumnDescriptor),
					columnFamilyHandles);

			defaultColumnFamily = columnFamilyHandles.get(0);

			ColumnFamilyDescriptor processingTimeColumnFamilyDescriptor =
				new ColumnFamilyDescriptor(EVENT_TIME_NAME, columnFamilyOptions);
			processingTimeColumnFamily = db.createColumnFamily(processingTimeColumnFamilyDescriptor);

			ColumnFamilyDescriptor eventTimeColumnFamilyDescriptor =
				new ColumnFamilyDescriptor(PROCESSING_TIME_NAME, columnFamilyOptions);
			eventTimeColumnFamily = db.createColumnFamily(eventTimeColumnFamilyDescriptor);

		} catch (RocksDBException e) {
			throw new IOException("Error while creating the RocksDB instance.", e);
		}
	}

	@Override
	protected InternalTimerService<K, N> createInternalTimerService(
			String serviceName,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {

		return new RocksDBInternalTimerService<>(serviceName,
			totalKeyGroups, localKeyGroupRange, keySerializer, namespaceSerializer,
			keyContext, processingTimeService,
			db, processingTimeColumnFamily, eventTimeColumnFamily, writeOptions);
	}

	@Override
	public void dispose() {
		if (db != null) {
			IOUtils.closeQuietly(eventTimeColumnFamily);
			IOUtils.closeQuietly(processingTimeColumnFamily);
			IOUtils.closeQuietly(defaultColumnFamily);
			IOUtils.closeQuietly(db);
			IOUtils.closeQuietly(writeOptions);
			IOUtils.closeQuietly(columnFamilyOptions);
			IOUtils.closeQuietly(dbOptions);

			db = null;
		}

		FileUtils.deleteDirectoryQuietly(dbDirectory);

		super.dispose();
	}

	/**
	 * Sets the path for db storage.
	 *
	 * @param paths The paths where the rocksdb locates.
	 */
	@VisibleForTesting
	public void setDbStoragePaths(File... paths) {
		baseDirectories = paths;
	}

	// ------------------------------------------------------------------------
	//  static library loading utilities
	// ------------------------------------------------------------------------

	private void ensureRocksDBIsLoaded(String tempDirectory) throws IOException {
		synchronized (RocksDBInternalTimeServiceManager.class) {
			if (!rocksDbInitialized) {

				final File tempDirParent = new File(tempDirectory).getAbsoluteFile();
				LOG.info("Attempting to load RocksDB native library and store it under '{}'", tempDirParent);

				Throwable lastException = null;
				for (int attempt = 1; attempt <= ROCKSDB_LIB_LOADING_ATTEMPTS; attempt++) {
					try {
						// when multiple instances of this class and RocksDB exist in different
						// class loaders, then we can see the following exception:
						// "java.lang.UnsatisfiedLinkError: Native Library /path/to/temp/dir/librocksdbjni-linux64.so
						// already loaded in another class loader"

						// to avoid that, we need to add a random element to the library file path
						// (I know, seems like an unnecessary hack, since the JVM obviously can handle multiple
						//  instances of the same JNI library being loaded in different class loaders, but
						//  apparently not when coming from the same file path, so there we go)

						final File rocksLibFolder = new File(tempDirParent, "rocksdb-lib-" + new AbstractID());

						// make sure the temp path exists
						LOG.debug("Attempting to create RocksDB native library folder {}", rocksLibFolder);
						// noinspection ResultOfMethodCallIgnored
						rocksLibFolder.mkdirs();

						// explicitly load the JNI dependency if it has not been loaded before
						NativeLibraryLoader.getInstance().loadLibrary(rocksLibFolder.getAbsolutePath());

						// this initialization here should validate that the loading succeeded
						RocksDB.loadLibrary();

						// seems to have worked
						LOG.info("Successfully loaded RocksDB native library");
						rocksDbInitialized = true;
						return;
					}
					catch (Throwable t) {
						lastException = t;
						LOG.debug("RocksDB JNI library loading attempt {} failed", attempt, t);

						// try to force RocksDB to attempt reloading the library
						try {
							resetRocksDBLoadedFlag();
						} catch (Throwable tt) {
							LOG.debug("Failed to reset 'initialized' flag in RocksDB native code loader", tt);
						}
					}
				}

				throw new IOException("Could not load the native RocksDB library", lastException);
			}
		}
	}

	@VisibleForTesting
	private static void resetRocksDBLoadedFlag() throws Exception {
		final Field initField = org.rocksdb.NativeLibraryLoader.class.getDeclaredField("initialized");
		initField.setAccessible(true);
		initField.setBoolean(null, false);
	}
}

