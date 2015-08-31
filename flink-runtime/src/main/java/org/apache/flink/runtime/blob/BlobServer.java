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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.JobID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the BLOB server. The BLOB server is responsible for listening for incoming requests and
 * spawning threads to handle these requests. Furthermore, it takes care of creating the directory structure to store
 * the BLOBs or temporarily cache them.
 */
public class BlobServer extends Thread implements BlobService {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(BlobServer.class);

	/** Counter to generate unique names for temporary files. */
	private final AtomicInteger tempFileCounter = new AtomicInteger(0);

	/** The server socket listening for incoming connections. */
	private final ServerSocket serverSocket;

	/** Indicates whether a shutdown of server component has been requested. */
	private final AtomicBoolean shutdownRequested = new AtomicBoolean();

	/** Shutdown hook thread to ensure deletion of the storage directory. */
	private final Thread shutdownHook;

	/** Is the root directory for file storage */
	private final File storageDir;

	/** Set of currently running threads */
	private final Set<BlobServerConnection> activeConnections = new HashSet<BlobServerConnection>();

	/** The maximum number of concurrent connections */
	private final int maxConnections;

	/**
	 * Instantiates a new BLOB server and binds it to a free network port.
	 * 
	 * @throws IOException
	 *         thrown if the BLOB server cannot bind to a free network port
	 */
	public BlobServer(Configuration config) throws IOException {

		// configure and create the storage directory
		String storageDirectory = config.getString(ConfigConstants.BLOB_STORAGE_DIRECTORY_KEY, null);
		this.storageDir = BlobUtils.initStorageDirectory(storageDirectory);
		LOG.info("Created BLOB server storage directory {}", storageDir);

		// configure the maximum number of concurrent connections
		final int maxConnections = config.getInteger(
				ConfigConstants.BLOB_FETCH_CONCURRENT_KEY, ConfigConstants.DEFAULT_BLOB_FETCH_CONCURRENT);
		if (maxConnections >= 1) {
			this.maxConnections = maxConnections;
		}
		else {
			LOG.warn("Invalid value for maximum connections in BLOB server: {}. Using default value of {}",
					maxConnections, ConfigConstants.DEFAULT_BLOB_FETCH_CONCURRENT);
			this.maxConnections = ConfigConstants.DEFAULT_BLOB_FETCH_CONCURRENT;
		}

		// configure the backlog of connections
		int backlog = config.getInteger(ConfigConstants.BLOB_FETCH_BACKLOG_KEY, ConfigConstants.DEFAULT_BLOB_FETCH_BACKLOG);
		if (backlog < 1) {
			LOG.warn("Invalid value for BLOB connection backlog: {}. Using default value of {}",
					backlog, ConfigConstants.DEFAULT_BLOB_FETCH_BACKLOG);
			backlog = ConfigConstants.DEFAULT_BLOB_FETCH_BACKLOG;
		}

		// Add shutdown hook to delete storage directory
		this.shutdownHook = BlobUtils.addShutdownHook(this, LOG);

		// start the server
		try {
			this.serverSocket = new ServerSocket(0, backlog);
		}
		catch (IOException e) {
			throw new IOException("Could not create BlobServer with automatic port choice.", e);
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
	 * @param key identifying the file
	 * @return file handle to the file
	 */
	public File getStorageLocation(BlobKey key) {
		return BlobUtils.getStorageLocation(storageDir, key);
	}

	/**
	 * Returns a file handle to the file identified by the given jobID and key.
	 *
	 * @param jobID to which the file is associated
	 * @param key to identify the file within the job context
	 * @return file handle to the file
	 */
	public File getStorageLocation(JobID jobID, String key) {
		return BlobUtils.getStorageLocation(storageDir, jobID, key);
	}

	/**
	 * Method which deletes all files associated with the given jobID.
	 *
	 * @param jobID all files associated to this jobID will be deleted
	 * @throws IOException
	 */
	public void deleteJobDirectory(JobID jobID) throws IOException {
		BlobUtils.deleteJobDirectory(storageDir, jobID);
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
				shutdown();
			}
		}
	}

	/**
	 * Shuts down the BLOB server.
	 */
	@Override
	public void shutdown() {
		if (shutdownRequested.compareAndSet(false, true)) {
			try {
				this.serverSocket.close();
			}
			catch (IOException ioe) {
				LOG.debug("Error while closing the server socket.", ioe);
			}

			// wake the thread up, in case it is waiting on some operation
			interrupt();

			try {
				join();
			}
			catch (InterruptedException ie) {
				LOG.debug("Error while waiting for this thread to die.", ie);
			}

			synchronized (activeConnections) {
				if (!activeConnections.isEmpty()) {
					for (BlobServerConnection conn : activeConnections) {
						LOG.debug("Shutting down connection " + conn.getName());
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
				LOG.error("BLOB server failed to properly clean up its storage directory.");
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
					LOG.warn("Exception while unregistering BLOB server's cleanup shutdown hook.");
				}
			}

			if(LOG.isInfoEnabled()) {
				LOG.info("Stopped BLOB server at {}:{}", serverSocket.getInetAddress().getHostAddress(), getPort());
			}
		}
	}

	/**
	 * Method which retrieves the URL of a file associated with a blob key. The blob server looks
	 * the blob key up in its local storage. If the file exists, then the URL is returned. If the
	 * file does not exist, then a FileNotFoundException is thrown.
	 *
	 * @param requiredBlob blob key associated with the requested file
	 * @return URL of the file
	 * @throws IOException
	 */
	@Override
	public URL getURL(BlobKey requiredBlob) throws IOException {
		if (requiredBlob == null) {
			throw new IllegalArgumentException("Required BLOB cannot be null.");
		}

		final File localFile = BlobUtils.getStorageLocation(storageDir, requiredBlob);

		if (!localFile.exists()) {
			throw new FileNotFoundException("File " + localFile.getCanonicalPath() + " does not exist.");
		} else {
			return localFile.toURI().toURL();
		}
	}

	/**
	 * This method deletes the file associated to the blob key if it exists in the local storage
	 * of the blob server.
	 *
	 * @param key associated with the file to be deleted
	 * @throws IOException
	 */
	@Override
	public void delete(BlobKey key) throws IOException {
		final File localFile = BlobUtils.getStorageLocation(storageDir, key);

		if (localFile.exists()) {
			if (!localFile.delete()) {
				LOG.warn("Failed to delete locally BLOB " + key + " at " + localFile.getAbsolutePath());
			}
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
