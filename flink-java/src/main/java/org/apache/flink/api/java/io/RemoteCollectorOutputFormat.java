/**
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
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

/**
 * An output format that sends results through JAVA RMI to an
 * {@link IRemoteCollector} implementation. The client has to provide an
 * implementation of {@link IRemoteCollector} and has to write it's plan's
 * output into an instance of {@link RemoteCollectorOutputFormat}. Further in
 * the client's VM parameters -Djava.rmi.server.hostname should be set to the
 * own IP address.
 */
public class RemoteCollectorOutputFormat<T> implements OutputFormat<T> {

	private static final long serialVersionUID = 1922744224032398102L;

	/**
	 * The reference of the {@link IRemoteCollector} object
	 */
	private IRemoteCollector<T> remoteCollector;

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
	public static final String ID = "RemoteCollector";

	@SuppressWarnings("unchecked")
	@Override
	/**
	 * This method receives the Configuration object, where the fields "remote" and "port" must be set.
	 */
	public void configure(Configuration parameters) {

		Registry registry = null;
		// get the remote's RMI Registry
		try {
			registry = LocateRegistry.getRegistry(
					parameters.getString(REMOTE, "localhost"),
					parameters.getInteger(PORT, 8888));

			// try to get an intance of an IRemoteCollector implementation
			this.remoteCollector = (IRemoteCollector<T>) registry.lookup(ID);
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
	}

	/**
	 * This method forwards records simply to the remote's
	 * {@link IRemoteCollector} implementation
	 */
	@Override
	public void writeRecord(T record) throws IOException {
		remoteCollector.collect(record);
	}

	/**
	 * This method unbinds the reference of the implementation of
	 * {@link IRemoteCollector}.
	 */
	@Override
	public void close() throws IOException {
	}

	@Override
	public String toString() {
		return "RemoteCollectorOutputFormat(" + remote + ":" + port + ", " + rmiId + ")";
	}
}
