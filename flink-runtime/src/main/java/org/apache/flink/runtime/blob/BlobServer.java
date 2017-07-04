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

package org.apache.flink.runtime.blob;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class implements the BLOB server. The BLOB server is responsible for listening for incoming requests and
 * spawning threads to handle these requests. Furthermore, it takes care of creating the directory structure to store
 * the BLOBs or temporarily cache them.
 */
public class BlobServer extends Thread implements BlobService {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(BlobServer.class);

	/** Counter to generate unique names for temporary files. */
	private final AtomicLong tempFileCounter = new AtomicLong(0);

	/** The server socket listening for incoming connections. */
	private final ServerSocket serverSocket;

	/** The SSL server context if ssl is enabled for the connections */
	private SSLContext serverSSLContext = null;

	/** Blob Server configuration */
	private final Configuration blobServiceConfiguration;

	/** Indicates whether a shutdown of server component has been requested. */
	private final AtomicBoolean shutdownRequested = new AtomicBoolean();

	/** Root directory for local file storage */
	private final File storageDir;

	/** Blob store for distributed file storage, e.g. in HA */
	private final BlobStore blobStore;

	/** Set of currently running threads */
	private final Set<BlobServerConnection> activeConnections = new HashSet<>();

	/** The maximum number of concurrent connections */
	private final int maxConnections;

	/** Lock guarding concurrent file accesses */
	private final ReadWriteLock readWriteLock;

	/**
	 * Shutdown hook thread to ensure deletion of the storage directory (or <code>null</code> if
	 * the configured high availability mode does not equal{@link HighAvailabilityMode#NONE})
	 */
	private final Thread shutdownHook;

	/**
	 * Instantiates a new BLOB server and binds it to a free network port.
	 *
	 * @param config Configuration to be used to instantiate the BlobServer
	 * @param blobStore BlobStore to store blobs persistently
	 *
	 * @throws IOException
	 * 		thrown if the BLOB server cannot bind to a free network port or if the
	 * 		(local or distributed) file storage cannot be created or is not usable
	 */
	public BlobServer(Configuration config, BlobStore blobStore) throws IOException {
		this.blobServiceConfiguration = checkNotNull(config);
		this.blobStore = checkNotNull(blobStore);
		this.readWriteLock = new ReentrantReadWriteLock();

		// configure and create the storage directory
		String storageDirectory = config.getString(BlobServerOptions.STORAGE_DIRECTORY);
		this.storageDir = BlobUtils.initLocalStorageDirectory(storageDirectory);
		LOG.info("Created BLOB server storage directory {}", storageDir);

		// configure the maximum number of concurrent connections
		final int maxConnections = config.getInteger(BlobServerOptions.FETCH_CONCURRENT);
		if (maxConnections >= 1) {
			this.maxConnections = maxConnections;
		}
		else {
			LOG.warn("Invalid value for maximum connections in BLOB server: {}. Using default value of {}",
					maxConnections, BlobServerOptions.FETCH_CONCURRENT.defaultValue());
			this.maxConnections = BlobServerOptions.FETCH_CONCURRENT.defaultValue();
		}

		// configure the backlog of connections
		int backlog = config.getInteger(BlobServerOptions.FETCH_BACKLOG);
		if (backlog < 1) {
			LOG.warn("Invalid value for BLOB connection backlog: {}. Using default value of {}",
					backlog, BlobServerOptions.FETCH_BACKLOG.defaultValue());
			backlog = BlobServerOptions.FETCH_BACKLOG.defaultValue();
		}

		this.shutdownHook = BlobUtils.addShutdownHook(this, LOG);

		if (config.getBoolean(BlobServerOptions.SSL_ENABLED)) {
			try {
				serverSSLContext = SSLUtils.createSSLServerContext(config);
			} catch (Exception e) {
				throw new IOException("Failed to initialize SSLContext for the blob server", e);
			}
		}

		//  ----------------------- start the server -------------------

		String serverPortRange = config.getString(BlobServerOptions.PORT);

		Iterator<Integer> ports = NetUtils.getPortRangeFromString(serverPortRange);

		final int finalBacklog = backlog;
		ServerSocket socketAttempt = NetUtils.createSocketFromPorts(ports, new NetUtils.SocketFactory() {
			@Override
			public ServerSocket createSocket(int port) throws IOException {
				if (serverSSLContext == null) {
					return new ServerSocket(port, finalBacklog);
				} else {
					LOG.info("Enabling ssl for the blob server");
					return serverSSLContext.getServerSocketFactory().createServerSocket(port, finalBacklog);
				}
			}
		});

		if(socketAttempt == null) {
			throw new IOException("Unable to allocate socket for blob server in specified port range: "+serverPortRange);
		} else {
			SSLUtils.setSSLVerAndCipherSuites(socketAttempt, config);
			this.serverSocket = socketAttempt;
		}

		// start the server thread
		setName("BLOB Server listener at " + getPort());
		setDaemon(true);
		start();

		if (LOG.isInfoEnabled()) {
			LOG.info("Started BLOB server at {}:{} - max concurrent requests: {} - max backlog: {}",
					serverSocket.getInetAddress().getHostAddress(), getPort(), maxConnections, backlog);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Path Accessors
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns a file handle to the file associated with the given blob key on the blob
	 * server.
	 *
	 * <p><strong>This is only called from the {@link BlobServerConnection}</strong>
	 *
	 * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param key identifying the file
	 * @return file handle to the file
	 */
	@VisibleForTesting
	public File getStorageLocation(JobID jobId, BlobKey key) {
		return BlobUtils.getStorageLocation(storageDir, jobId, key);
	}

	/**
	 * Returns a temporary file inside the BLOB server's incoming directory.
	 *
	 * @return a temporary file inside the BLOB server's incoming directory
	 */
	File createTemporaryFilename() {
		return new File(BlobUtils.getIncomingDirectory(storageDir),
				String.format("temp-%08d", tempFileCounter.getAndIncrement()));
	}

	/**
	 * Returns the blob store.
	 */
	BlobStore getBlobStore() {
		return blobStore;
	}

	/**
	 * Returns the lock used to guard file accesses
	 */
	public ReadWriteLock getReadWriteLock() {
		return readWriteLock;
	}

	@Override
	public void run() {
		try {
			while (!this.shutdownRequested.get()) {
				BlobServerConnection conn = new BlobServerConnection(serverSocket.accept(), this);
				try {
					synchronized (activeConnections) {
						while (activeConnections.size() >= maxConnections) {
							activeConnections.wait(2000);
						}
						activeConnections.add(conn);
					}

					conn.start();
					conn = null;
				}
				finally {
					if (conn != null) {
						conn.close();
						synchronized (activeConnections) {
							activeConnections.remove(conn);
						}
					}
				}
			}
		}
		catch (Throwable t) {
			if (!this.shutdownRequested.get()) {
				LOG.error("BLOB server stopped working. Shutting down", t);

				try {
					close();
				} catch (Throwable closeThrowable) {
					LOG.error("Could not properly close the BlobServer.", closeThrowable);
				}
			}
		}
	}

	/**
	 * Shuts down the BLOB server.
	 */
	@Override
	public void close() throws IOException {
		if (shutdownRequested.compareAndSet(false, true)) {
			Exception exception = null;

			try {
				this.serverSocket.close();
			}
			catch (IOException ioe) {
				exception = ioe;
			}

			// wake the thread up, in case it is waiting on some operation
			interrupt();

			try {
				join();
			}
			catch (InterruptedException ie) {
				Thread.currentThread().interrupt();

				LOG.debug("Error while waiting for this thread to die.", ie);
			}

			synchronized (activeConnections) {
				if (!activeConnections.isEmpty()) {
					for (BlobServerConnection conn : activeConnections) {
						LOG.debug("Shutting down connection {}.", conn.getName());
						conn.close();
					}
					activeConnections.clear();
				}
			}

			// Clean up the storage directory
			try {
				FileUtils.deleteDirectory(storageDir);
			}
			catch (IOException e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			// Remove shutdown hook to prevent resource leaks, unless this is invoked by the
			// shutdown hook itself
			if (shutdownHook != null && shutdownHook != Thread.currentThread()) {
				try {
					Runtime.getRuntime().removeShutdownHook(shutdownHook);
				}
				catch (IllegalStateException e) {
					// race, JVM is in shutdown already, we can safely ignore this
				}
				catch (Throwable t) {
					LOG.warn("Exception while unregistering BLOB server's cleanup shutdown hook.", t);
				}
			}

			if(LOG.isInfoEnabled()) {
				LOG.info("Stopped BLOB server at {}:{}", serverSocket.getInetAddress().getHostAddress(), getPort());
			}

			ExceptionUtils.tryRethrowIOException(exception);
		}
	}

	@Override
	public BlobClient createClient() throws IOException {
		return new BlobClient(new InetSocketAddress(serverSocket.getInetAddress(), getPort()),
			blobServiceConfiguration);
	}

	/**
	 * Retrieves the local path of a (job-unrelated) file associated with a job and a blob key.
	 * <p>
	 * The blob server looks the blob key up in its local storage. If the file exists, it is
	 * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
	 * or a {@link FileNotFoundException} is thrown.
	 *
	 * @param key
	 * 		blob key associated with the requested file
	 *
	 * @return file referring to the local storage location of the BLOB
	 *
	 * @throws IOException
	 * 		Thrown if the file retrieval failed.
	 */
	@Override
	public File getFile(BlobKey key) throws IOException {
		return getFileInternal(null, key);
	}

	/**
	 * Retrieves the local path of a file associated with a job and a blob key.
	 * <p>
	 * The blob server looks the blob key up in its local storage. If the file exists, it is
	 * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
	 * or a {@link FileNotFoundException} is thrown.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		blob key associated with the requested file
	 *
	 * @return file referring to the local storage location of the BLOB
	 *
	 * @throws IOException
	 * 		Thrown if the file retrieval failed.
	 */
	@Override
	public File getFile(JobID jobId, BlobKey key) throws IOException {
		checkNotNull(jobId);
		return getFileInternal(jobId, key);
	}

	/**
	 * Retrieves the local path of a file associated with a job and a blob key.
	 * <p>
	 * The blob server looks the blob key up in its local storage. If the file exists, it is
	 * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
	 * or a {@link FileNotFoundException} is thrown.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param requiredBlob
	 * 		blob key associated with the requested file
	 *
	 * @return file referring to the local storage location of the BLOB
	 *
	 * @throws IOException
	 * 		Thrown if the file retrieval failed.
	 */
	private File getFileInternal(@Nullable JobID jobId, BlobKey requiredBlob) throws IOException {
		checkArgument(requiredBlob != null, "BLOB key cannot be null.");

		final File localFile = BlobUtils.getStorageLocation(storageDir, jobId, requiredBlob);

		if (localFile.exists()) {
			return localFile;
		}
		else {
			try {
				// Try the blob store
				blobStore.get(jobId, requiredBlob, localFile);
			}
			catch (Exception e) {
				throw new IOException(
					"Failed to copy BLOB " + requiredBlob + " from blob store to " + localFile, e);
			}

			if (localFile.exists()) {
				return localFile;
			}
			else {
				throw new FileNotFoundException("Local file " + localFile + " does not exist " +
						"and failed to copy from blob store.");
			}
		}
	}

	/**
	 * Deletes the (job-unrelated) file associated with the blob key in both the local storage as
	 * well as in the HA store of the blob server.
	 *
	 * @param key
	 * 		blob key associated with the file to be deleted
	 *
	 * @throws IOException
	 */
	@Override
	public void delete(BlobKey key) throws IOException {
		deleteInternal(null, key);
	}

	/**
	 * Deletes the file associated with the blob key in both the local storage as well as in the HA
	 * store of the blob server.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		blob key associated with the file to be deleted
	 *
	 * @throws IOException
	 */
	@Override
	public void delete(JobID jobId, BlobKey key) throws IOException {
		checkNotNull(jobId);
		deleteInternal(jobId, key);
	}

	/**
	 * Deletes the file associated with the blob key in both the local storage as well as in the HA
	 * store of the blob server.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param key
	 * 		blob key associated with the file to be deleted
	 *
	 * @throws IOException
	 */
	void deleteInternal(@Nullable JobID jobId, BlobKey key) throws IOException {
		final File localFile = BlobUtils.getStorageLocation(storageDir, jobId, key);

		readWriteLock.writeLock().lock();

		try {
			if (!localFile.delete() && localFile.exists()) {
				LOG.warn("Failed to locally delete BLOB " + key + " at " + localFile.getAbsolutePath());
			}

			blobStore.delete(jobId, key);
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}

	/**
	 * Removes all BLOBs from local and HA store belonging to the given job ID.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 */
	public void cleanupJob(JobID jobId) {
		checkNotNull(jobId);

		final File jobDir =
			new File(BlobUtils.getStorageLocationPath(storageDir.getAbsolutePath(), jobId));

		readWriteLock.writeLock().lock();

		try {
			// delete locally
			try {
				FileUtils.deleteDirectory(jobDir);
			} catch (IOException e) {
				LOG.warn("Failed to locally delete BLOB storage directory at " +
					jobDir.getAbsolutePath(), e);
			}

			// delete in HA store
			blobStore.deleteAll(jobId);
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}


	/**
	 * Returns the port on which the server is listening.
	 *
	 * @return port on which the server is listening
	 */
	@Override
	public int getPort() {
		return this.serverSocket.getLocalPort();
	}

	/**
	 * Tests whether the BLOB server has been requested to shut down.
	 *
	 * @return True, if the server has been requested to shut down, false otherwise.
	 */
	public boolean isShutdown() {
		return this.shutdownRequested.get();
	}

	/**
	 * Access to the server socket, for testing
	 */
	ServerSocket getServerSocket() {
		return this.serverSocket;
	}

	void unregisterConnection(BlobServerConnection conn) {
		synchronized (activeConnections) {
			activeConnections.remove(conn);
			activeConnections.notifyAll();
		}
	}

	/**
	 * Returns all the current active connections in the BlobServer.
	 *
	 * @return the list of all the active in current BlobServer
	 */
	List<BlobServerConnection> getCurrentActiveConnections() {
		synchronized (activeConnections) {
			return new ArrayList<BlobServerConnection>(activeConnections);
		}
	}

}
