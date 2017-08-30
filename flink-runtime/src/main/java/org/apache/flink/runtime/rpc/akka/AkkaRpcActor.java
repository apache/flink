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

import akka.actor.ActorRef;
import akka.actor.Status;
import akka.actor.UntypedActor;
import akka.japi.Procedure;
import akka.pattern.Patterns;
import org.apache.flink.runtime.rpc.MainThreadValidatorUtil;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.akka.exceptions.AkkaRpcException;
import org.apache.flink.runtime.rpc.akka.messages.CallAsync;
import org.apache.flink.runtime.rpc.akka.messages.LocalRpcInvocation;
import org.apache.flink.runtime.rpc.akka.messages.Processing;
import org.apache.flink.runtime.rpc.akka.messages.RpcInvocation;
import org.apache.flink.runtime.rpc.akka.messages.RunAsync;

import org.apache.flink.runtime.rpc.akka.messages.Shutdown;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.impl.Promise;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Akka rpc actor which receives {@link LocalRpcInvocation}, {@link RunAsync} and {@link CallAsync}
 * {@link Processing} messages.
 * <p>
 * The {@link LocalRpcInvocation} designates a rpc and is dispatched to the given {@link RpcEndpoint}
 * instance.
 * <p>
 * The {@link RunAsync} and {@link CallAsync} messages contain executable code which is executed
 * in the context of the actor thread.
 * <p>
 * The {@link Processing} message controls the processing behaviour of the akka rpc actor. A
 * {@link Processing#START} starts processing incoming messages. A {@link Processing#STOP} message
 * stops processing messages. All messages which arrive when the processing is stopped, will be
 * discarded.
 *
 * @param <T> Type of the {@link RpcEndpoint}
 */
class AkkaRpcActor<T extends RpcEndpoint & RpcGateway> extends UntypedActor {
	
	private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcActor.class);

	/** the endpoint to invoke the methods on */
	private final T rpcEndpoint;

	/** the helper that tracks whether calls come from the main thread */
	private final MainThreadValidatorUtil mainThreadValidator;

	private final CompletableFuture<Void> terminationFuture;

	/** Throwable which might have been thrown by the postStop method */
	private Throwable shutdownThrowable;

	AkkaRpcActor(final T rpcEndpoint, final CompletableFuture<Void> terminationFuture) {
		this.rpcEndpoint = checkNotNull(rpcEndpoint, "rpc endpoint");
		this.mainThreadValidator = new MainThreadValidatorUtil(rpcEndpoint);
		this.terminationFuture = checkNotNull(terminationFuture);

		this.shutdownThrowable = null;
	}

	@Override
	public void postStop() throws Exception {
		super.postStop();

		// IMPORTANT: This only works if we don't use a restarting supervisor strategy. Otherwise
		// we would complete the future and let the actor system restart the actor with a completed
		// future.
		// Complete the termination future so that others know that we've stopped.

		if (shutdownThrowable != null) {
			terminationFuture.completeExceptionally(shutdownThrowable);
		} else {
			terminationFuture.complete(null);
		}
	}

	@Override
	public void onReceive(final Object message) {
		if (message.equals(Processing.START)) {
			getContext().become(new Procedure<Object>() {
				@Override
				public void apply(Object msg) throws Exception {
					if (msg.equals(Processing.STOP)) {
						getContext().unbecome();
					} else {
						handleMessage(msg);
					}
				}
			});
		} else {
			LOG.info("The rpc endpoint {} has not been started yet. Discarding message {} until processing is started.",
				rpcEndpoint.getClass().getName(),
				message.getClass().getName());

			if (!getSender().equals(ActorRef.noSender())) {
				// fail a possible future if we have a sender
				getSender().tell(new Status.Failure(new AkkaRpcException("Discard message, because " +
					"the rpc endpoint has not been started yet.")), getSelf());
			}
		}
	}

	private void handleMessage(Object message) {
		mainThreadValidator.enterMainThread();
		try {
			if (message instanceof RunAsync) {
				handleRunAsync((RunAsync) message);
			} else if (message instanceof CallAsync) {
				handleCallAsync((CallAsync) message);
			} else if (message instanceof RpcInvocation) {
				handleRpcInvocation((RpcInvocation) message);
			} else if (message instanceof Shutdown) {
				triggerShutdown();
			} else {
				LOG.warn(
					"Received message of unknown type {} with value {}. Dropping this message!",
					message.getClass().getName(),
					message);
			}
		} finally {
			mainThreadValidator.exitMainThread();
		}
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
		} catch(ClassNotFoundException e) {
			LOG.error("Could not load method arguments.", e);

			RpcConnectionException rpcException = new RpcConnectionException("Could not load method arguments.", e);
			getSender().tell(new Status.Failure(rpcException), getSelf());
		} catch (IOException e) {
			LOG.error("Could not deserialize rpc invocation message.", e);

			RpcConnectionException rpcException = new RpcConnectionException("Could not deserialize rpc invocation message.", e);
			getSender().tell(new Status.Failure(rpcException), getSelf());
		} catch (final NoSuchMethodException e) {
			LOG.error("Could not find rpc method for rpc invocation.", e);

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
						LOG.trace("Reporting back error thrown in remote procedure {}", rpcMethod, e);

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
				LOG.error("Error while executing remote procedure call {}.", rpcMethod, e);
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

			LOG.warn(result);

			getSender().tell(new Status.Failure(new Exception(result)), getSelf());
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
			LOG.warn("Received a {} message with an empty runnable field. This indicates " +
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
					LOG.error("Caught exception while executing runnable in main thread.", t);
					ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
				}
			}
			else {
				// schedule for later. send a new message after the delay, which will then be immediately executed 
				FiniteDuration delay = new FiniteDuration(delayNanos, TimeUnit.NANOSECONDS);
				RunAsync message = new RunAsync(runAsync.getRunnable(), timeToRun);

				getContext().system().scheduler().scheduleOnce(delay, getSelf(), message,
						getContext().dispatcher(), ActorRef.noSender());
			}
		}
	}

	private void triggerShutdown() {
		try {
			rpcEndpoint.postStop();
		} catch (Throwable throwable) {
			shutdownThrowable = throwable;
		}

		// now stop the actor which will stop processing of any further messages
		getContext().system().stop(getSelf());
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
}
