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

package org.apache.flink.api.java.io;

import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

/**
 * An output format that sends results through JAVA RMI to an
 * {@link RemoteCollector} implementation. The client has to provide an
 * implementation of {@link RemoteCollector} and has to write it's plan's output
 * into an instance of {@link RemoteCollectorOutputFormat}. Further in the
 * client's VM parameters -Djava.rmi.server.hostname should be set to the own IP
 * address.
 */
public class RemoteCollectorOutputFormat<T> implements OutputFormat<T> {

	private static final long serialVersionUID = 1922744224032398102L;

	/**
	 * The reference of the {@link RemoteCollector} object
	 */
	private transient RemoteCollector<T> remoteCollector;

	transient private Registry registry;

	/**
	 * Config parameter for the remote's port number
	 */
	public static final String PORT = "port";
	/**
	 * Config parameter for the remote's address
	 */
	public static final String REMOTE = "remote";
	/**
	 * An id used necessary for Java RMI
	 */
	public static final String RMI_ID = "rmiId";

	private String remote;

	private int port;

	private String rmiId;

	/**
	 * Create a new {@link RemoteCollectorOutputFormat} instance. The remote and
	 * port for this output are by default localhost:8888 but can be configured
	 * via a {@link Configuration} object.
	 * 
	 * @see RemoteCollectorOutputFormat#REMOTE
	 * @see RemoteCollectorOutputFormat#PORT
	 */
	public RemoteCollectorOutputFormat() {
		this("localhost", 8888, null);
	}

	/**
	 * Creates a new {@link RemoteCollectorOutputFormat} instance for the
	 * specified remote and port.
	 * 
	 * @param rmiId
	 */
	public RemoteCollectorOutputFormat(String remote, int port, String rmiId) {
		super();
		this.remote = remote;
		this.port = port;
		this.rmiId = rmiId;
		
		if (this.remote == null) {
			throw new IllegalStateException(String.format(
					"No remote configured for %s.", this));
		}

		if (this.rmiId == null) {
			throw new IllegalStateException(String.format(
					"No registry ID configured for %s.", this));
		}
	}

	@Override
	/**
	 * This method receives the Configuration object, where the fields "remote" and "port" must be set.
	 */
	public void configure(Configuration parameters) {
		this.remote = parameters.getString(REMOTE, this.remote);
		this.port = parameters.getInteger(PORT, this.port);
		this.rmiId = parameters.getString(RMI_ID, this.rmiId);

		if (this.remote == null) {
			throw new IllegalStateException(String.format(
					"No remote configured for %s.", this));
		}

		if (this.rmiId == null) {
			throw new IllegalStateException(String.format(
					"No registry ID configured for %s.", this));
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		// get the remote's RMI Registry
		try {
			registry = LocateRegistry.getRegistry(this.remote, this.port);
		} catch (RemoteException e) {
			throw new IllegalStateException(e);
		}

		// try to get an intance of an IRemoteCollector implementation
		try {
			this.remoteCollector = (RemoteCollector<T>) registry
					.lookup(this.rmiId);
		} catch (AccessException e) {
			throw new IllegalStateException(e);
		} catch (RemoteException e) {
			throw new IllegalStateException(e);
		} catch (NotBoundException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * This method forwards records simply to the remote's
	 * {@link RemoteCollector} implementation
	 */
	@Override
	public void writeRecord(T record) throws IOException {
		remoteCollector.collect(record);
	}

	/**
	 * This method unbinds the reference of the implementation of
	 * {@link RemoteCollector}.
	 */
	@Override
	public void close() throws IOException {
	}

	@Override
	public String toString() {
		return "RemoteCollectorOutputFormat(" + remote + ":" + port + ", "
				+ rmiId + ")";
	}

}
