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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.management.remote.rmi.RMIJRMPServerImpl;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.atomic.AtomicReference;


/**
 * JMX Server implementation that JMX clients can connect to.
 *
 * <p>Heavily based on j256 simplejmx project
 *
 * <p>https://github.com/j256/simplejmx/blob/master/src/main/java/com/j256/simplejmx/server/JmxServer.java
 */
class JMXServer {
	private static final Logger LOG = LoggerFactory.getLogger(JMXServer.class);

	private final AtomicReference<Remote> rmiServerReference = new AtomicReference<>();

	private Registry rmiRegistry;
	private JMXConnectorServer connector;
	private int port;

	JMXServer() {
	}

	void start(int port) throws IOException {
		if (rmiRegistry != null && connector != null) {
			LOG.debug("JMXServer is already running.");
			return;
		}
		internalStart(port);
		this.port = port;
	}

	void stop() throws IOException {
		rmiServerReference.set(null);
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

	int getPort() {
		return port;
	}

	private void internalStart(int port) throws IOException {
		rmiServerReference.set(null);

		// this allows clients to lookup the JMX service
		rmiRegistry = new JmxRegistry(port, "jmxrmi", rmiServerReference);

		String serviceUrl = "service:jmx:rmi://localhost:" + port + "/jndi/rmi://localhost:" + port + "/jmxrmi";
		JMXServiceURL url;
		try {
			url = new JMXServiceURL(serviceUrl);
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException("Malformed service url created " + serviceUrl, e);
		}

		final RMIJRMPServerImpl rmiServer = new RMIJRMPServerImpl(port, null, null, null);

		connector = new RMIConnectorServer(url, null, rmiServer, ManagementFactory.getPlatformMBeanServer());
		connector.start();

		// we can't pass the created stub directly to the registry since this would form a cyclic dependency:
		// - you can only start the connector after the registry was started
		// - you can only create the stub after the connector was started
		// - you can only start the registry after the stub was created
		rmiServerReference.set(rmiServer.toStub());
	}

	/**
	 * A registry that only exposes a single remote object.
	 */
	@SuppressWarnings("restriction")
	private static class JmxRegistry extends sun.rmi.registry.RegistryImpl {
		private final String lookupName;
		private final AtomicReference<Remote> remoteServerStub;

		JmxRegistry(final int port, final String lookupName, final AtomicReference<Remote> remoteServerStub) throws RemoteException {
			super(port);
			this.lookupName = lookupName;
			this.remoteServerStub = remoteServerStub;
		}

		@Override
		public Remote lookup(String s) throws NotBoundException {
			if (lookupName.equals(s)) {
				final Remote remote = remoteServerStub.get();
				if (remote != null) {
					return remote;
				}
			}
			throw new NotBoundException("Not bound.");
		}

		@Override
		public void bind(String s, Remote remote) {
			// this is called from RMIConnectorServer#start; don't throw a general AccessException
		}

		@Override
		public void unbind(String s) {
			// this is called from RMIConnectorServer#stop; don't throw a general AccessException
		}

		@Override
		public void rebind(String s, Remote remote) {
			// might as well not throw an exception here given that the others don't
		}

		@Override
		public String[] list() {
			return new String[]{lookupName};
		}
	}
}
