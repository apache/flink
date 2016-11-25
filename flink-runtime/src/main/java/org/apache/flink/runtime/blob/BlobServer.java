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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class implements the BLOB server. The BLOB server is responsible for listening for incoming requests and
 * spawning threads to handle these requests. Furthermore, it takes care of creating the directory structure to store
 * the BLOBs or temporarily cache them.
 */
public class BlobServer extends Thread implements BlobService {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(BlobServer.class);

	/** The server socket listening for incoming connections. */
	private final ServerSocket serverSocket;

	/** The SSL server context if ssl is enabled for the connections */
	private SSLContext serverSSLContext = null;

	/** Blob Server configuration */
	private final Configuration blobServiceConfiguration;

	/** Indicates whether a shutdown of server component has been requested. */
	private final AtomicBoolean shutdownRequested = new AtomicBoolean();

	/** Blob store to use */
	private final BlobStore blobStore;

	/** Set of currently running threads */
	private final Set<BlobServerConnection> activeConnections = new HashSet<>();

	/** The maximum number of concurrent connections */
	private final int maxConnections;

	/**
	 * Shutdown hook thread to ensure deletion of the storage directory (or <code>null</code> if
	 * the configured high availability mode does not equal{@link HighAvailabilityMode#NONE})
	 */
	private final Thread shutdownHook;

	/**
	 * Instantiates a new BLOB server and binds it to a free network port.
	 *
	 * @param config global configuration
	 * @throws IOException
	 *         thrown if the BLOB server cannot bind to a free network port or
	 *         the file storage cannot be created
	 */
	public BlobServer(Configuration config) throws IOException {
		checkNotNull(config, "Configuration");

		this.blobServiceConfiguration = config;

		// configure and create the storage directory
		this.blobStore = new FileSystemBlobStore(config, true);

		try {

			// configure the maximum number of concurrent connections
			final int maxConnections = config.getInteger(
				ConfigConstants.BLOB_FETCH_CONCURRENT_KEY, ConfigConstants.DEFAULT_BLOB_FETCH_CONCURRENT);
			if (maxConnections >= 1) {
				this.maxConnections = maxConnections;
			} else {
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

			if (config.getBoolean(ConfigConstants.BLOB_SERVICE_SSL_ENABLED,
				ConfigConstants.DEFAULT_BLOB_SERVICE_SSL_ENABLED)) {
				try {
					serverSSLContext = SSLUtils.createSSLServerContext(config);
				} catch (Exception e) {
					throw new IOException("Failed to initialize SSLContext for the blob server", e);
				}
			}

			//  ----------------------- start the server -------------------

			String serverPortRange = config.getString(ConfigConstants.BLOB_SERVER_PORT, ConfigConstants.DEFAULT_BLOB_SERVER_PORT);

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

			if (socketAttempt == null) {
				throw new IOException("Unable to allocate socket for blob server in specified port range: " + serverPortRange);
			} else {
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
		} catch (Exception e) {
			if (blobStore != null) {
				blobStore.cleanUp();
			}
			throw e;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Path Accessors
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the blob store.
	 */
	BlobStore getBlobStore() {
		return blobStore;
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
			blobStore.cleanUp();

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

	@Override
	public BlobClient createClient() throws IOException {
		return new BlobClient(new InetSocketAddress(serverSocket.getInetAddress(), getPort()),
			blobServiceConfiguration);
	}

	/**
	 * Method which retrieves the URL of a file associated with a blob key.
	 *
	 * If the file exists in the blob store, its URL is returned. If the
	 * file does not exist, a FileNotFoundException is thrown.
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

		return blobStore.getFileStatus(requiredBlob).getPath().toUri().toURL();
	}

	/**
	 * This method deletes the file associated to the blob key from the storage
	 * (local or distributed depending on the HA mode).
	 *
	 * @param key associated with the file to be deleted
	 * @throws IOException
	 */
	@Override
	public void delete(BlobKey key) throws IOException {
		blobStore.delete(key);
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
