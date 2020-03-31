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

package org.apache.flink.runtime.management;

import org.apache.flink.configuration.JMXServerOptions;
import org.apache.flink.util.NetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Iterator;


/**
 * JMX Server implementation that JMX clients can connect to.
 *
 * <p>Heavily based on j256 simplejmx project
 *
 * <p>https://github.com/j256/simplejmx/blob/master/src/main/java/com/j256/simplejmx/server/JmxServer.java
 */
public class JMXServer {
	private static final Logger LOG = LoggerFactory.getLogger(JMXServer.class);

	private static JMXServer instance = null;

	private Registry rmiRegistry;
	private JMXConnectorServer connector;
	private int port;

	/**
	 * Construct a new JMV-wide JMX server or acquire existing JMX server.
	 *
	 * <p>If JMXServer static instance is already constructed, it will not be
	 * reconstruct again. Instead a warning sign will be posted if the desired
	 * port configuration doesn't match the existing JMXServer static instance.
	 *
	 * @param portsConfig port configuration of the JMX server.
	 * @return JMXServer static instance.
	 */
	public static JMXServer startInstance(String portsConfig) {
		if (instance == null) {
			if (!portsConfig.equals(JMXServerOptions.JMX_SERVER_PORT.defaultValue())) {
				instance = startJMXServerWithPortRanges(portsConfig);
			} else {
				LOG.warn("JMX Server start failed. No explicit JMX port is configured.");
				instance = null;
			}
		} else {
			LOG.warn("JVM-wide JMXServer already started at port: " + instance.port);
		}
		return instance;
	}

	/**
	 * Acquire existing JMX server. or null if not started.
	 *
	 * @return the JMXServer static instance.
	 */
	public static JMXServer getInstance() {
		return instance;
	}

	/**
	 * Stop the JMX server.
	 */
	public static void stopInstance() throws IOException {
		if (instance != null) {
			instance.stop();
		}
	}

	public static int getPort() {
		if (instance != null) {
			return instance.port;
		} else {
			return -1;
		}
	}

	private static JMXServer startJMXServerWithPortRanges(String portsConfig) {
		Iterator<Integer> ports = NetUtils.getPortRangeFromString(portsConfig);
		JMXServer successfullyStartedServer = null;
		while (ports.hasNext() && successfullyStartedServer == null) {
			JMXServer server = new JMXServer();
			int port = ports.next();
			try {
				server.start(port);
				LOG.info("Started JMX server on port " + port + ".");
				successfullyStartedServer = server;
			} catch (IOException ioe) { //assume port conflict
				LOG.debug("Could not start JMX server on port " + port + ".", ioe);
				try {
					server.stop();
				} catch (Exception e) {
					LOG.debug("Could not stop JMX server.", e);
				}
			}
		}
		if (successfullyStartedServer == null) {
			throw new RuntimeException("Could not start JMX server on any configured port. Ports: " + portsConfig);
		}
		return successfullyStartedServer;
	}

	private void start(int port) throws IOException {
		if (rmiRegistry != null && connector != null) {
			LOG.debug("JMXServer is already running.");
			return;
		}
		startRmiRegistry(port);
		startJmxService(port);
		this.port = port;
	}

	private void stop() throws IOException {
		if (connector != null) {
			try {
				connector.stop();
			} finally {
				connector = null;
			}
		}
		if (rmiRegistry != null) {
			try {
				UnicastRemoteObject.unexportObject(rmiRegistry, true);
			} catch (NoSuchObjectException e) {
				throw new IOException("Could not un-export our RMI registry", e);
			} finally {
				rmiRegistry = null;
			}
		}
	}

	/**
	 * Starts an RMI Registry that allows clients to lookup the JMX IP/port.
	 *
	 * @param port rmi port to use
	 * @throws IOException
	 */
	private void startRmiRegistry(int port) throws IOException {
		rmiRegistry = LocateRegistry.createRegistry(port);
	}

	/**
	 * Starts a JMX connector that allows (un)registering MBeans with the MBean server and RMI invocations.
	 *
	 * @param port jmx port to use
	 * @throws IOException
	 */
	private void startJmxService(int port) throws IOException {
		String serviceUrl = "service:jmx:rmi://localhost:" + port + "/jndi/rmi://localhost:" + port + "/jmxrmi";
		JMXServiceURL url;
		try {
			url = new JMXServiceURL(serviceUrl);
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException("Malformed service url created " + serviceUrl, e);
		}

		connector = JMXConnectorServerFactory.newJMXConnectorServer(url, null, ManagementFactory.getPlatformMBeanServer());

		connector.start();
	}
}
