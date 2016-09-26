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

package org.apache.flink.runtime.registration;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.concurrent.AcceptFunction;
import org.apache.flink.runtime.concurrent.ApplyFunction;
import org.apache.flink.runtime.concurrent.Future;

import org.slf4j.Logger;

import java.util.UUID;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This utility class implements the basis of RPC connecting from one component to another component,
 * for example the RPC connection from TaskExecutor to ResourceManager.
 * This {@code RegisteredRpcConnection} implements registration and get target gateway .
 *
 * <p>The registration gives access to a future that is completed upon successful registration.
 * The RPC connection can be closed, for example when the target where it tries to register
 * at looses leader status.
 *
 * @param <Gateway> The type of the gateway to connect to.
 * @param <Success> The type of the successful registration responses.
 */
public abstract class RegisteredRpcConnection<Gateway extends RpcGateway, Success extends RegistrationResponse.Success> {

	/** the logger for all log messages of this class */
	protected final Logger log;

	/** the target component leaderID, for example the ResourceManager leaderID */
	private final UUID targetLeaderId;

	/** the target component Address, for example the ResourceManager Address */
	private final String targetAddress;

	/** Execution context to be used to execute the on complete action of the ResourceManagerRegistration */
	private final Executor executor;

	/** the Registration of this RPC connection */
	private RetryingRegistration<Gateway, Success> pendingRegistration;

	/** the gateway to register, it's null until the registration is completed */
	private volatile Gateway targetGateway;

	/** flag indicating that the RPC connection is closed */
	private volatile boolean closed;

	// ------------------------------------------------------------------------

	public RegisteredRpcConnection(
		Logger log,
		String targetAddress,
		UUID targetLeaderId,
		Executor executor)
	{
		this.log = checkNotNull(log);
		this.targetAddress = checkNotNull(targetAddress);
		this.targetLeaderId = checkNotNull(targetLeaderId);
		this.executor = checkNotNull(executor);
	}

	// ------------------------------------------------------------------------
	//  Life cycle
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	public void start() {
		checkState(!closed, "The RPC connection is already closed");
		checkState(!isConnected() && pendingRegistration == null, "The RPC connection is already started");

		pendingRegistration = checkNotNull(generateRegistration());
		pendingRegistration.startRegistration();

		Future<Tuple2<Gateway, Success>> future = pendingRegistration.getFuture();

		future.thenAcceptAsync(new AcceptFunction<Tuple2<Gateway, Success>>() {
			@Override
			public void accept(Tuple2<Gateway, Success> result) {
				targetGateway = result.f0;
				onRegistrationSuccess(result.f1);
			}
		}, executor);

		// this future should only ever fail if there is a bug, not if the registration is declined
		future.exceptionallyAsync(new ApplyFunction<Throwable, Void>() {
			@Override
			public Void apply(Throwable failure) {
				onRegistrationFailure(failure);
				return null;
			}
		}, executor);
	}

	/**
	 * This method generate a specific Registration, for example TaskExecutor Registration at the ResourceManager
	 */
	protected abstract RetryingRegistration<Gateway, Success> generateRegistration();

	/**
	 * This method handle the Registration Response
	 */
	protected abstract void onRegistrationSuccess(Success success);

	/**
	 * This method handle the Registration failure
	 */
	protected abstract void onRegistrationFailure(Throwable failure);

	/**
	 * close connection
	 */
	public void close() {
		closed = true;

		// make sure we do not keep re-trying forever
		if (pendingRegistration != null) {
			pendingRegistration.cancel();
		}
	}

	public boolean isClosed() {
		return closed;
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public UUID getTargetLeaderId() {
		return targetLeaderId;
	}

	public String getTargetAddress() {
		return targetAddress;
	}

	/**
	 * Gets the RegisteredGateway. This returns null until the registration is completed.
	 */
	public Gateway getTargetGateway() {
		return targetGateway;
	}

	public boolean isConnected() {
		return targetGateway != null;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		String connectionInfo = "(ADDRESS: " + targetAddress + " LEADERID: " + targetLeaderId + ")";

		if (isConnected()) {
			connectionInfo = "RPC connection to " + targetGateway.getClass().getSimpleName() + " " + connectionInfo;
		} else {
			connectionInfo = "RPC connection to " + connectionInfo;
		}

		if (isClosed()) {
			connectionInfo = connectionInfo + " is closed";
		} else if (isConnected()){
			connectionInfo = connectionInfo + " is established";
		} else {
			connectionInfo = connectionInfo + " is connecting";
		}

		return connectionInfo;
	}
}
