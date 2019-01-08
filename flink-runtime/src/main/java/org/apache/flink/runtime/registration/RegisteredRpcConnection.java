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

import org.slf4j.Logger;

import java.io.Serializable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This utility class implements the basis of RPC connecting from one component to another component,
 * for example the RPC connection from TaskExecutor to ResourceManager.
 * This {@code RegisteredRpcConnection} implements registration and get target gateway.
 *
 * <p>The registration gives access to a future that is completed upon successful registration.
 * The RPC connection can be closed, for example when the target where it tries to register
 * at looses leader status.
 *
 * @param <F> The type of the fencing token
 * @param <G> The type of the gateway to connect to.
 * @param <S> The type of the successful registration responses.
 */
public abstract class RegisteredRpcConnection<F extends Serializable, G extends RpcGateway, S extends RegistrationResponse.Success> {

	private static final AtomicReferenceFieldUpdater<RegisteredRpcConnection, RetryingRegistration> REGISTRATION_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
		RegisteredRpcConnection.class,
		RetryingRegistration.class,
		"pendingRegistration");

	/** The logger for all log messages of this class. */
	protected final Logger log;

	/** The fencing token fo the remote component. */
	private final F fencingToken;

	/** The target component Address, for example the ResourceManager Address. */
	private final String targetAddress;

	/** Execution context to be used to execute the on complete action of the ResourceManagerRegistration. */
	private final Executor executor;

	/** The Registration of this RPC connection. */
	private volatile RetryingRegistration<F, G, S> pendingRegistration;

	/** The gateway to register, it's null until the registration is completed. */
	private volatile G targetGateway;

	/** Flag indicating that the RPC connection is closed. */
	private volatile boolean closed;

	// ------------------------------------------------------------------------

	public RegisteredRpcConnection(Logger log, String targetAddress, F fencingToken, Executor executor) {
		this.log = checkNotNull(log);
		this.targetAddress = checkNotNull(targetAddress);
		this.fencingToken = checkNotNull(fencingToken);
		this.executor = checkNotNull(executor);
	}

	// ------------------------------------------------------------------------
	//  Life cycle
	// ------------------------------------------------------------------------

	public void start() {
		checkState(!closed, "The RPC connection is already closed");
		checkState(!isConnected() && pendingRegistration == null, "The RPC connection is already started");

		final RetryingRegistration<F, G, S> newRegistration = createNewRegistration();

		if (REGISTRATION_UPDATER.compareAndSet(this, null, newRegistration)) {
			newRegistration.startRegistration();
		} else {
			// concurrent start operation
			newRegistration.cancel();
		}
	}

	public boolean tryReconnect() {
		checkState(isConnected(), "Cannot reconnect to an unknown destination.");

		if (closed) {
			return false;
		} else {
			final RetryingRegistration<F, G, S> currentPendingRegistration = pendingRegistration;

			if (currentPendingRegistration != null) {
				currentPendingRegistration.cancel();
			}

			final RetryingRegistration<F, G, S> newRegistration = createNewRegistration();

			if (REGISTRATION_UPDATER.compareAndSet(this, currentPendingRegistration, newRegistration)) {
				newRegistration.startRegistration();
			} else {
				// concurrent modification
				newRegistration.cancel();
				return false;
			}

			// double check for concurrent close operations
			if (closed) {
				newRegistration.cancel();

				return false;
			} else {
				return true;
			}
		}
	}

	/**
	 * This method generate a specific Registration, for example TaskExecutor Registration at the ResourceManager.
	 */
	protected abstract RetryingRegistration<F, G, S> generateRegistration();

	/**
	 * This method handle the Registration Response.
	 */
	protected abstract void onRegistrationSuccess(S success);

	/**
	 * This method handle the Registration failure.
	 */
	protected abstract void onRegistrationFailure(Throwable failure);

	/**
	 * Close connection.
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

	public F getTargetLeaderId() {
		return fencingToken;
	}

	public String getTargetAddress() {
		return targetAddress;
	}

	/**
	 * Gets the RegisteredGateway. This returns null until the registration is completed.
	 */
	public G getTargetGateway() {
		return targetGateway;
	}

	public boolean isConnected() {
		return targetGateway != null;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		String connectionInfo = "(ADDRESS: " + targetAddress + " FENCINGTOKEN: " + fencingToken + ")";

		if (isConnected()) {
			connectionInfo = "RPC connection to " + targetGateway.getClass().getSimpleName() + " " + connectionInfo;
		} else {
			connectionInfo = "RPC connection to " + connectionInfo;
		}

		if (isClosed()) {
			connectionInfo += " is closed";
		} else if (isConnected()){
			connectionInfo += " is established";
		} else {
			connectionInfo += " is connecting";
		}

		return connectionInfo;
	}

	// ------------------------------------------------------------------------
	//  Internal methods
	// ------------------------------------------------------------------------

	private RetryingRegistration<F, G, S> createNewRegistration() {
		RetryingRegistration<F, G, S> newRegistration = checkNotNull(generateRegistration());

		CompletableFuture<Tuple2<G, S>> future = newRegistration.getFuture();

		future.whenCompleteAsync(
			(Tuple2<G, S> result, Throwable failure) -> {
				if (failure != null) {
					if (failure instanceof CancellationException) {
						// we ignore cancellation exceptions because they originate from cancelling
						// the RetryingRegistration
						log.debug("Retrying registration towards {} was cancelled.", targetAddress);
					} else {
						// this future should only ever fail if there is a bug, not if the registration is declined
						onRegistrationFailure(failure);
					}
				} else {
					targetGateway = result.f0;
					onRegistrationSuccess(result.f1);
				}
			}, executor);

		return newRegistration;
	}
}
