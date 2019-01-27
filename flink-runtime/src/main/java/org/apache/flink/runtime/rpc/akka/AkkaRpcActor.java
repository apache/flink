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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rpc.MainThreadValidatorUtil;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.akka.exceptions.AkkaHandshakeException;
import org.apache.flink.runtime.rpc.akka.exceptions.AkkaRpcException;
import org.apache.flink.runtime.rpc.akka.exceptions.AkkaUnknownMessageException;
import org.apache.flink.runtime.rpc.akka.messages.Processing;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.messages.CallAsync;
import org.apache.flink.runtime.rpc.messages.HandshakeSuccessMessage;
import org.apache.flink.runtime.rpc.messages.LocalRpcInvocation;
import org.apache.flink.runtime.rpc.messages.RemoteHandshakeMessage;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.runtime.rpc.messages.RunAsync;
import org.apache.flink.util.ExceptionUtils;

import akka.actor.ActorRef;
import akka.actor.Status;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.impl.Promise;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Akka rpc actor which receives {@link LocalRpcInvocation}, {@link RunAsync} and {@link CallAsync}
 * {@link Processing} messages.
 *
 * <p>The {@link LocalRpcInvocation} designates a rpc and is dispatched to the given {@link RpcEndpoint}
 * instance.
 *
 * <p>The {@link RunAsync} and {@link CallAsync} messages contain executable code which is executed
 * in the context of the actor thread.
 *
 * <p>The {@link Processing} message controls the processing behaviour of the akka rpc actor. A
 * {@link Processing#START} starts processing incoming messages. A {@link Processing#STOP} message
 * stops processing messages. All messages which arrive when the processing is stopped, will be
 * discarded.
 *
 * @param <T> Type of the {@link RpcEndpoint}
 */
class AkkaRpcActor<T extends RpcEndpoint & RpcGateway> extends UntypedActor {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	/** the endpoint to invoke the methods on. */
	protected final T rpcEndpoint;

	/** the helper that tracks whether calls come from the main thread. */
	private final MainThreadValidatorUtil mainThreadValidator;

	private final CompletableFuture<Boolean> terminationFuture;

	private final int version;

	private State state;

	AkkaRpcActor(final T rpcEndpoint, final CompletableFuture<Boolean> terminationFuture, final int version) {
		this.rpcEndpoint = checkNotNull(rpcEndpoint, "rpc endpoint");
		this.mainThreadValidator = new MainThreadValidatorUtil(rpcEndpoint);
		this.terminationFuture = checkNotNull(terminationFuture);
		this.version = version;
		this.state = State.STOPPED;
	}

	@Override
	public void postStop() throws Exception {
		mainThreadValidator.enterMainThread();

		try {
			CompletableFuture<Void> postStopFuture;
			try {
				postStopFuture = rpcEndpoint.postStop();
			} catch (Throwable throwable) {
				postStopFuture = FutureUtils.completedExceptionally(throwable);
			}

			super.postStop();

			// IMPORTANT: This only works if we don't use a restarting supervisor strategy. Otherwise
			// we would complete the future and let the actor system restart the actor with a completed
			// future.
			// Complete the termination future so that others know that we've stopped.

			postStopFuture.whenComplete(
				(Void value, Throwable throwable) -> {
					if (throwable != null) {
						terminationFuture.completeExceptionally(throwable);
					} else {
						terminationFuture.complete(null);
					}
				});
		} finally {
			mainThreadValidator.exitMainThread();
		}
	}

	@Override
	public void onReceive(final Object message) {
		if (message instanceof RemoteHandshakeMessage) {
			handleHandshakeMessage((RemoteHandshakeMessage) message);
		} else if (message.equals(Processing.START)) {
			state = State.STARTED;
		} else if (message.equals(Processing.STOP)) {
			state = State.STOPPED;
		} else if (state == State.STARTED) {
			mainThreadValidator.enterMainThread();

			try {
				handleRpcMessage(message);
			} finally {
				mainThreadValidator.exitMainThread();
			}
		} else {
			log.info("The rpc endpoint {} has not been started yet. Discarding message {} until processing is started.",
				rpcEndpoint.getClass().getName(),
				message.getClass().getName());

			sendErrorIfSender(new AkkaRpcException(
				String.format("Discard message, because the rpc endpoint %s has not been started yet.", rpcEndpoint.getAddress())));
		}
	}

	protected void handleRpcMessage(Object message) {
		if (message instanceof RunAsync) {
			handleRunAsync((RunAsync) message);
		} else if (message instanceof CallAsync) {
			handleCallAsync((CallAsync) message);
		} else if (message instanceof RpcInvocation) {
			handleRpcInvocation((RpcInvocation) message);
		} else {
			log.warn(
				"Received message of unknown type {} with value {}. Dropping this message!",
				message.getClass().getName(),
				message);

			sendErrorIfSender(new AkkaUnknownMessageException("Received unknown message " + message +
				" of type " + message.getClass().getSimpleName() + '.'));
		}
	}

	private void handleHandshakeMessage(RemoteHandshakeMessage handshakeMessage) {
		if (!isCompatibleVersion(handshakeMessage.getVersion())) {
			sendErrorIfSender(new AkkaHandshakeException(
				String.format(
					"Version mismatch between source (%s) and target (%s) rpc component. Please verify that all components have the same version.",
					handshakeMessage.getVersion(),
					getVersion())));
		} else if (!isGatewaySupported(handshakeMessage.getRpcGateway())) {
			sendErrorIfSender(new AkkaHandshakeException(
				String.format(
					"The rpc endpoint does not support the gateway %s.",
					handshakeMessage.getRpcGateway().getSimpleName())));
		} else {
			getSender().tell(new Status.Success(HandshakeSuccessMessage.INSTANCE), getSelf());
		}
	}

	private boolean isGatewaySupported(Class<?> rpcGateway) {
		return rpcGateway.isAssignableFrom(rpcEndpoint.getClass());
	}

	private boolean isCompatibleVersion(int sourceVersion) {
		return sourceVersion == getVersion();
	}

	private int getVersion() {
		return version;
	}

	/**
	 * Handle rpc invocations by looking up the rpc method on the rpc endpoint and calling this
	 * method with the provided method arguments. If the method has a return value, it is returned
	 * to the sender of the call.
	 *
	 * @param rpcInvocation Rpc invocation message
	 */
	private void handleRpcInvocation(RpcInvocation rpcInvocation) {
		Method rpcMethod = null;

		try {
			String methodName = rpcInvocation.getMethodName();
			Class<?>[] parameterTypes = rpcInvocation.getParameterTypes();

			rpcMethod = lookupRpcMethod(methodName, parameterTypes);
		} catch (ClassNotFoundException e) {
			log.error("Could not load method arguments.", e);

			RpcConnectionException rpcException = new RpcConnectionException("Could not load method arguments.", e);
			getSender().tell(new Status.Failure(rpcException), getSelf());
		} catch (IOException e) {
			log.error("Could not deserialize rpc invocation message.", e);

			RpcConnectionException rpcException = new RpcConnectionException("Could not deserialize rpc invocation message.", e);
			getSender().tell(new Status.Failure(rpcException), getSelf());
		} catch (final NoSuchMethodException e) {
			log.error("Could not find rpc method for rpc invocation.", e);

			RpcConnectionException rpcException = new RpcConnectionException("Could not find rpc method for rpc invocation.", e);
			getSender().tell(new Status.Failure(rpcException), getSelf());
		}

		if (rpcMethod != null) {
			try {
				// this supports declaration of anonymous classes
				rpcMethod.setAccessible(true);

				if (rpcMethod.getReturnType().equals(Void.TYPE)) {
					// No return value to send back
					rpcMethod.invoke(rpcEndpoint, rpcInvocation.getArgs());
				}
				else {
					final Object result;
					try {
						result = rpcMethod.invoke(rpcEndpoint, rpcInvocation.getArgs());
					}
					catch (InvocationTargetException e) {
						log.trace("Reporting back error thrown in remote procedure {}", rpcMethod, e);

						// tell the sender about the failure
						getSender().tell(new Status.Failure(e.getTargetException()), getSelf());
						return;
					}

					if (result instanceof CompletableFuture) {
						final CompletableFuture<?> future = (CompletableFuture<?>) result;
						Promise.DefaultPromise<Object> promise = new Promise.DefaultPromise<>();

						future.whenComplete(
							(value, throwable) -> {
								if (throwable != null) {
									promise.failure(throwable);
								} else {
									promise.success(value);
								}
							});

						Patterns.pipe(promise.future(), getContext().dispatcher()).to(getSender());
					} else {
						// tell the sender the result of the computation
						getSender().tell(new Status.Success(result), getSelf());
					}
				}
			} catch (Throwable e) {
				log.error("Error while executing remote procedure call {}.", rpcMethod, e);
				// tell the sender about the failure
				getSender().tell(new Status.Failure(e), getSelf());
			}
		}
	}

	/**
	 * Handle asynchronous {@link Callable}. This method simply executes the given {@link Callable}
	 * in the context of the actor thread.
	 *
	 * @param callAsync Call async message
	 */
	private void handleCallAsync(CallAsync callAsync) {
		if (callAsync.getCallable() == null) {
			final String result = "Received a " + callAsync.getClass().getName() + " message with an empty " +
				"callable field. This indicates that this message has been serialized " +
				"prior to sending the message. The " + callAsync.getClass().getName() +
				" is only supported with local communication.";

			log.warn(result);

			getSender().tell(new Status.Failure(new AkkaRpcException(result)), getSelf());
		} else {
			try {
				Object result = callAsync.getCallable().call();

				getSender().tell(new Status.Success(result), getSelf());
			} catch (Throwable e) {
				getSender().tell(new Status.Failure(e), getSelf());
			}
		}
	}

	/**
	 * Handle asynchronous {@link Runnable}. This method simply executes the given {@link Runnable}
	 * in the context of the actor thread.
	 *
	 * @param runAsync Run async message
	 */
	private void handleRunAsync(RunAsync runAsync) {
		if (runAsync.getRunnable() == null) {
			log.warn("Received a {} message with an empty runnable field. This indicates " +
				"that this message has been serialized prior to sending the message. The " +
				"{} is only supported with local communication.",
				runAsync.getClass().getName(),
				runAsync.getClass().getName());
		}
		else {
			final long timeToRun = runAsync.getTimeNanos();
			final long delayNanos;

			if (timeToRun == 0 || (delayNanos = timeToRun - System.nanoTime()) <= 0) {
				// run immediately
				try {
					runAsync.getRunnable().run();
				} catch (Throwable t) {
					log.error("Caught exception while executing runnable in main thread.", t);
					ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
				}
			}
			else {
				// schedule for later. send a new message after the delay, which will then be immediately executed
				FiniteDuration delay = new FiniteDuration(delayNanos, TimeUnit.NANOSECONDS);
				RunAsync message = new RunAsync(runAsync.getRunnable(), timeToRun);

				final Object envelopedSelfMessage = envelopeSelfMessage(message);

				getContext().system().scheduler().scheduleOnce(delay, getSelf(), envelopedSelfMessage,
						getContext().dispatcher(), ActorRef.noSender());
			}
		}
	}

	/**
	 * Look up the rpc method on the given {@link RpcEndpoint} instance.
	 *
	 * @param methodName Name of the method
	 * @param parameterTypes Parameter types of the method
	 * @return Method of the rpc endpoint
	 * @throws NoSuchMethodException Thrown if the method with the given name and parameter types
	 * 									cannot be found at the rpc endpoint
	 */
	private Method lookupRpcMethod(final String methodName, final Class<?>[] parameterTypes) throws NoSuchMethodException {
		return rpcEndpoint.getClass().getMethod(methodName, parameterTypes);
	}

	/**
	 * Send throwable to sender if the sender is specified.
	 *
	 * @param throwable to send to the sender
	 */
	protected void sendErrorIfSender(Throwable throwable) {
		if (!getSender().equals(ActorRef.noSender())) {
			getSender().tell(new Status.Failure(throwable), getSelf());
		}
	}

	/**
	 * Hook to envelope self messages.
	 *
	 * @param message to envelope
	 * @return enveloped message
	 */
	protected Object envelopeSelfMessage(Object message) {
		return message;
	}

	enum State {
		STARTED,
		STOPPED
	}
}
