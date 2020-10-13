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

package org.apache.flink.connectors.test.common.source;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.time.Duration;
import java.util.List;

/**
 * A wrapped controller for controlling {@link ControllableSource}, which handles Java RMI connecting and wraps all RPC
 * methods for pre-condition checking and logging.
 */
public class SourceController implements SourceControlRpc {

	private static final Logger LOG = LoggerFactory.getLogger(SourceController.class);

	private SourceControlRpc stub;
	private String host;
	private List<Integer> potentialPorts;
	private boolean connected;

	public SourceController(List<Integer> potentialPorts) {
		this(ControllableSource.RMI_HOSTNAME, potentialPorts);
	}

	public SourceController(String host, List<Integer> potentialPorts) {
		this.host = host;
		this.potentialPorts = potentialPorts;
		connected = false;
	}

	private SourceController() {
	}

	/**
	 * Connect to the controllable source running remotely via Java RMI.
	 */
	public void connect() {
		int actualRMIPort = -1;

		for (Integer port : potentialPorts) {
			try {
				stub = (SourceControlRpc) LocateRegistry.getRegistry(
						host,
						port
				).lookup("SourceControl");
				actualRMIPort = port;
				break;
			} catch (NotBoundException e) {
				// This isn't the task manager we want. Just skip it
			} catch (RemoteException e) {
				throw new IllegalStateException("Error when getting registry on " + host + ":" + port, e);
			}
		}

		if (stub == null || actualRMIPort == -1) {
			throw new IllegalStateException("Cannot find any controllable source among task managers");
		}

		LOG.info("Connected to controllable source at {}:{}", host, actualRMIPort);

		// Because of the mechanism of Java RMI, host and port registered in RMI registry would be LOCAL inside docker,
		// which is not accessible on docker host / testing framework.
		// So we "hack" into the dynamic proxy object created by Java RMI to correct the port number using reflection.

		try {
			FieldUtils.writeField(
				FieldUtils.readField(
					FieldUtils.readField(
						FieldUtils.readField(
							FieldUtils.readField(stub, "h", true),
							"ref", true),
						"ref", true),
					"ep", true),
				"port", actualRMIPort, true);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Corrupted Java RMI remote object", e);
		}

		connected = true;

	}

	public void connect(Duration timeout) {
		long deadline = System.currentTimeMillis() + timeout.toMillis();
		while (System.currentTimeMillis() < deadline) {
			try {
				connect();
			} catch (Exception e) {
				LOG.debug("Retrying connecting to remote object...");
				continue;
			}
			// Successfully connected to controllable source, jump out of the loop directly
			break;
		}
		if (!connected) {
			throw new IllegalStateException("Cannot connect to controllable source within " + timeout);
		}
	}

	@Override
	public void pause() throws RemoteException {
		ensureConnected();
		LOG.trace("Sending PAUSE command...");
		stub.pause();
	}

	@Override
	public void next() throws RemoteException {
		ensureConnected();
		LOG.trace("Sending NEXT command...");
		stub.next();
	}

	@Override
	public void go() throws RemoteException {
		ensureConnected();
		LOG.trace("Sending GO command...");
		stub.go();
	}

	@Override
	public void finish() throws RemoteException {
		ensureConnected();
		LOG.trace("Sending FINISH command...");
		stub.finish();
	}

	private void ensureConnected() {
		if (!connected) {
			throw new IllegalStateException("Source controller is not connected to remote object");
		}
	}
}
