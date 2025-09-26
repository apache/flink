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

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.security.watch.LocalFSWatchServiceListener;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.NetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ServerSocketFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/** This class implements socket management (open, close) for the BLOB server. */
public class BlobServerSocket
        extends LocalFSWatchServiceListener.AbstractLocalFSWatchServiceListener {

    private static final Logger LOG = LoggerFactory.getLogger(BlobServerSocket.class);

    private final Configuration config;
    private final int backlog;
    private final String serverPortRange;
    private ServerSocket serverSocket;
    private final int maxConnections;
    private int reloadCounter = 0;
    private final AtomicBoolean firstCreation = new AtomicBoolean(true);

    public BlobServerSocket(Configuration config, int backlog, int maxConnections)
            throws IOException {
        this.config = config;
        this.backlog = backlog;
        this.maxConnections = maxConnections;

        serverPortRange = config.get(BlobServerOptions.PORT);
        createSocket();
    }

    public ServerSocket getServerSocket() {
        return serverSocket;
    }

    /**
     * Recreates a socket with a new ssl certificates.
     *
     * @return true if socket was recreated, false otherwise
     */
    public synchronized boolean reloadContextIfNeeded() {
        return reloadContextIfNeeded(this::reloadContext);
    }

    private void reloadContext() throws IOException {
        close();
        createSocket();
        reloadCounter++;
    }

    private synchronized void createSocket() throws IOException {
        Iterator<Integer> ports;
        if (firstCreation.get()) {
            ports = NetUtils.getPortRangeFromString(serverPortRange);
        } else {
            ports = Collections.singleton(serverSocket.getLocalPort()).iterator();
        }

        final ServerSocketFactory socketFactory;
        if (SecurityOptions.isInternalSSLEnabled(config)
                && config.get(BlobServerOptions.SSL_ENABLED)) {
            try {
                socketFactory = SSLUtils.createSSLServerSocketFactory(config);
            } catch (Exception e) {
                throw new IOException("Failed to initialize SSL for the blob server", e);
            }
        } else {
            socketFactory = ServerSocketFactory.getDefault();
        }

        final int finalBacklog = backlog;
        final String bindHost =
                config.getOptional(JobManagerOptions.BIND_HOST)
                        .orElseGet(NetUtils::getWildcardIPAddress);

        this.serverSocket =
                NetUtils.createSocketFromPorts(
                        ports,
                        (port) ->
                                socketFactory.createServerSocket(
                                        port, finalBacklog, InetAddress.getByName(bindHost)));

        if (serverSocket == null) {
            throw new IOException(
                    "Unable to open BLOB Server in specified port range: " + serverPortRange);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Started BLOB server at {}:{} - max concurrent requests: {} - max backlog: {}",
                    serverSocket.getInetAddress().getHostAddress(),
                    getPort(),
                    maxConnections,
                    backlog);
        }
        firstCreation.set(false);
    }

    /**
     * Returns the port on which the server is listening.
     *
     * @return port on which the server is listening
     */
    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public int getReloadCounter() {
        return reloadCounter;
    }

    public void close() throws IOException {
        if (serverSocket != null) {
            close(serverSocket);
        }
    }

    private void close(ServerSocket serverSocketToClose) throws IOException {
        if (LOG.isInfoEnabled()) {
            if (serverSocketToClose != null) {
                LOG.info(
                        "Stopped BLOB server at {}:{}",
                        serverSocketToClose.getInetAddress().getHostAddress(),
                        getPort());
            } else {
                LOG.info("Stopped BLOB server before initializing the socket");
            }
        }
        if (serverSocketToClose != null) {
            serverSocketToClose.close();
        }
    }
}
