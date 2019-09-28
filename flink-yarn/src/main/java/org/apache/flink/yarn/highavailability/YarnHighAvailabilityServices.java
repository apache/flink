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

package org.apache.flink.yarn.highavailability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.blob.FileSystemBlobStore;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The basis of {@link HighAvailabilityServices} for YARN setups.
 * These high-availability services auto-configure YARN's HDFS and the YARN application's
 * working directory to be used to store job recovery data.
 *
 * <p>Note for implementers: This class locks access to and creation of services,
 * to make sure all services are properly shut down when shutting down this class.
 * To participate in the checks, overriding methods should frame method body with
 * calls to {@code enter()} and {@code exit()} as shown in the following pattern:
 *
 * <pre>{@code
 * public LeaderRetrievalService getResourceManagerLeaderRetriever() {
 *     enter();
 *     try {
 *         CuratorClient client = getCuratorClient();
 *         return ZooKeeperUtils.createLeaderRetrievalService(client, configuration, RESOURCE_MANAGER_LEADER_PATH);
 *     } finally {
 *         exit();
 *     }
 * }
 * }</pre>
 */
public abstract class YarnHighAvailabilityServices implements HighAvailabilityServices {

	/** The name of the sub directory in which Flink stores the recovery data. */
	public static final String FLINK_RECOVERY_DATA_DIR = "flink_recovery_data";

	/** Logger for these services, shared with subclasses. */
	protected static final Logger LOG = LoggerFactory.getLogger(YarnHighAvailabilityServices.class);

	// ------------------------------------------------------------------------

	/** The lock that guards all accesses to methods in this class. */
	private final ReentrantLock lock;

	/** The Flink FileSystem object that represent the HDFS used by YARN. */
	protected final FileSystem flinkFileSystem;

	/** The Hadoop FileSystem object that represent the HDFS used by YARN. */
	protected final org.apache.hadoop.fs.FileSystem hadoopFileSystem;

	/** The working directory of this YARN application.
	 * This MUST NOT be deleted when the HA services clean up */
	protected final Path workingDirectory;

	/** The directory for HA persistent data. This should be deleted when the
	 * HA services clean up. */
	protected final Path haDataDirectory;

	/** Blob store service to be used for the BlobServer and BlobCache. */
	protected final BlobStoreService blobStoreService;

	/** Flag marking this instance as shut down. */
	private volatile boolean closed;

	// ------------------------------------------------------------------------

	/**
	 * Creates new YARN high-availability services, configuring the file system and recovery
	 * data directory based on the working directory in the given Hadoop configuration.
	 *
	 * <p>This class requires that the default Hadoop file system configured in the given
	 * Hadoop configuration is an HDFS.
	 *
	 * @param config     The Flink configuration of this component / process.
	 * @param hadoopConf The Hadoop configuration for the YARN cluster.
	 *
	 * @throws IOException Thrown, if the initialization of the Hadoop file system used by YARN fails.
	 */
	protected YarnHighAvailabilityServices(
			Configuration config,
			org.apache.hadoop.conf.Configuration hadoopConf) throws IOException {

		checkNotNull(config);
		checkNotNull(hadoopConf);

		this.lock = new ReentrantLock();

		// get and verify the YARN HDFS URI
		final URI fsUri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConf);
		if (fsUri.getScheme() == null || !"hdfs".equals(fsUri.getScheme().toLowerCase())) {
			throw new IOException("Invalid file system found for YarnHighAvailabilityServices: " +
					"Expected 'hdfs', but found '" + fsUri.getScheme() + "'.");
		}

		// initialize the Hadoop File System
		// we go through this special code path here to make sure we get no shared cached
		// instance of the FileSystem
		try {
			final Class<? extends org.apache.hadoop.fs.FileSystem> fsClass =
					org.apache.hadoop.fs.FileSystem.getFileSystemClass(fsUri.getScheme(), hadoopConf);

			this.hadoopFileSystem = InstantiationUtil.instantiate(fsClass);
			this.hadoopFileSystem.initialize(fsUri, hadoopConf);
		}
		catch (Exception e) {
			throw new IOException("Cannot instantiate YARN's Hadoop file system for " + fsUri, e);
		}

		this.flinkFileSystem = new HadoopFileSystem(hadoopFileSystem);

		this.workingDirectory = new Path(hadoopFileSystem.getWorkingDirectory().toUri());
		this.haDataDirectory = new Path(workingDirectory, FLINK_RECOVERY_DATA_DIR);

		// test the file system, to make sure we fail fast if access does not work
		try {
			flinkFileSystem.mkdirs(haDataDirectory);
		}
		catch (Exception e) {
			throw new IOException("Could not create the directory for recovery data in YARN's file system at '"
					+ haDataDirectory + "'.", e);
		}

		LOG.info("Flink YARN application will store recovery data at {}", haDataDirectory);

		blobStoreService = new FileSystemBlobStore(flinkFileSystem, haDataDirectory.toString());
	}

	// ------------------------------------------------------------------------
	//  high availability services
	// ------------------------------------------------------------------------

	@Override
	public BlobStore createBlobStore() throws IOException {
		enter();
		try {
			return blobStoreService;
		} finally {
			exit();
		}
	}

	// ------------------------------------------------------------------------
	//  Shutdown
	// ------------------------------------------------------------------------

	/**
	 * Checks whether these services have been shut down.
	 *
	 * @return True, if this instance has been shut down, false if it still operational.
	 */
	public boolean isClosed() {
		return closed;
	}

	@Override
	public void close() throws Exception {
		lock.lock();
		try {
			// close only once
			if (closed) {
				return;
			}
			closed = true;

			Throwable exception = null;

			try {
				blobStoreService.close();
			} catch (Throwable t) {
				exception = t;
			}

			// we do not propagate exceptions here, but only log them
			try {
				hadoopFileSystem.close();
			} catch (Throwable t) {
				exception = ExceptionUtils.firstOrSuppressed(t, exception);
			}

			if (exception != null) {
				ExceptionUtils.rethrowException(exception, "Could not properly close the YarnHighAvailabilityServices.");
			}
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public void closeAndCleanupAllData() throws Exception {
		lock.lock();
		try {
			checkState(!closed, "YarnHighAvailabilityServices are already closed");

			// we remember exceptions only, then continue cleanup, and re-throw at the end
			Throwable exception = null;

			try {
				blobStoreService.closeAndCleanupAllData();
			} catch (Throwable t) {
				exception = t;
			}

			// first, we delete all data in Flink's data directory
			try {
				flinkFileSystem.delete(haDataDirectory, true);
			}
			catch (Throwable t) {
				exception = ExceptionUtils.firstOrSuppressed(t, exception);
			}

			// now we actually close the services
			try {
				close();
			}
			catch (Throwable t) {
				exception = firstOrSuppressed(t, exception);
			}

			// if some exception occurred, rethrow
			if (exception != null) {
				ExceptionUtils.rethrowException(exception, exception.getMessage());
			}
		}
		finally {
			lock.unlock();
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * To be called at the beginning of every method that creates an HA service. Acquires the lock
	 * and check whether this HighAvailabilityServices instance is shut down.
	 */
	void enter() {
		if (!enterUnlessClosed()) {
			throw new IllegalStateException("closed");
		}
	}

	/**
	 * Acquires the lock and checks whether the services are already closed. If they are
	 * already closed, the method releases the lock and returns {@code false}.
	 *
	 * @return True, if the lock was acquired and the services are not closed, false if the services are closed.
	 */
	boolean enterUnlessClosed() {
		lock.lock();
		if (!closed) {
			return true;
		} else {
			lock.unlock();
			return false;
		}
	}

	/**
	 * To be called at the end of every method that creates an HA service. Releases the lock.
	 */
	void exit() {
		lock.unlock();
	}

	// ------------------------------------------------------------------------
	//  Factory from Configuration
	// ------------------------------------------------------------------------

	/**
	 * Creates the high-availability services for a single-job Flink YARN application, to be
	 * used in the Application Master that runs both ResourceManager and JobManager.
	 *
	 * @param flinkConfig  The Flink configuration.
	 * @param hadoopConfig The Hadoop configuration for the YARN cluster.
	 *
	 * @return The created high-availability services.
	 *
	 * @throws IOException Thrown, if the high-availability services could not be initialized.
	 */
	public static YarnHighAvailabilityServices forSingleJobAppMaster(
			Configuration flinkConfig,
			org.apache.hadoop.conf.Configuration hadoopConfig) throws IOException {

		checkNotNull(flinkConfig, "flinkConfig");
		checkNotNull(hadoopConfig, "hadoopConfig");

		final HighAvailabilityMode mode = HighAvailabilityMode.fromConfig(flinkConfig);
		switch (mode) {
			case NONE:
				return new YarnIntraNonHaMasterServices(flinkConfig, hadoopConfig);

			case ZOOKEEPER:
				throw  new UnsupportedOperationException("to be implemented");

			default:
				throw new IllegalConfigurationException("Unrecognized high availability mode: " + mode);
		}
	}

	/**
	 * Creates the high-availability services for the TaskManagers participating in
	 * a Flink YARN application.
	 *
	 * @param flinkConfig  The Flink configuration.
	 * @param hadoopConfig The Hadoop configuration for the YARN cluster.
	 *
	 * @return The created high-availability services.
	 *
	 * @throws IOException Thrown, if the high-availability services could not be initialized.
	 */
	public static YarnHighAvailabilityServices forYarnTaskManager(
			Configuration flinkConfig,
			org.apache.hadoop.conf.Configuration hadoopConfig) throws IOException {

		checkNotNull(flinkConfig, "flinkConfig");
		checkNotNull(hadoopConfig, "hadoopConfig");

		final HighAvailabilityMode mode = HighAvailabilityMode.fromConfig(flinkConfig);
		switch (mode) {
			case NONE:
				return new YarnPreConfiguredMasterNonHaServices(
					flinkConfig,
					hadoopConfig,
					HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION);

			case ZOOKEEPER:
				throw  new UnsupportedOperationException("to be implemented");

			default:
				throw new IllegalConfigurationException("Unrecognized high availability mode: " + mode);
		}
	}
}
