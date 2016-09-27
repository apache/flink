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
import akka.actor.ActorSelection;
import akka.actor.Status;
import akka.actor.Terminated;
import akka.actor.UntypedActorWithStash;
import akka.dispatch.Futures;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import akka.japi.Procedure;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkFuture;
import org.apache.flink.runtime.rpc.MainThreadValidatorUtil;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.akka.messages.CallAsync;
import org.apache.flink.runtime.rpc.akka.messages.LocalRpcInvocation;
import org.apache.flink.runtime.rpc.akka.messages.Processing;
import org.apache.flink.runtime.rpc.akka.messages.RpcInvocation;
import org.apache.flink.runtime.rpc.akka.messages.RunAsync;

import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.akka.messages.WatchOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
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
 * {@link Processing#START} message unstashes all stashed messages and starts processing incoming
 * messages. A {@link Processing#STOP} message stops processing messages and stashes incoming
 * messages.
 *
 * @param <C> Type of the {@link RpcGateway} associated with the {@link RpcEndpoint}
 * @param <T> Type of the {@link RpcEndpoint}
 */
class AkkaRpcActor<C extends RpcGateway, T extends RpcEndpoint<C>> extends UntypedActorWithStash {

	private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcActor.class);

	/** the endpoint to invoke the methods on */
	private final T rpcEndpoint;

	/** the helper that tracks whether calls come from the main thread */
	private final MainThreadValidatorUtil mainThreadValidator;

	/** watch actors and gateway relationship */
	private final Map<ActorRef, RpcGateway> watchActorRefs;

	AkkaRpcActor(final T rpcEndpoint) {
		this.rpcEndpoint = checkNotNull(rpcEndpoint, "rpc endpoint");
		this.mainThreadValidator = new MainThreadValidatorUtil(rpcEndpoint);
		this.watchActorRefs = new HashMap<>(16);
	}

	@Override
	public void onReceive(final Object message) {
		if (message.equals(Processing.START)) {
			unstashAll();
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
			LOG.info("The rpc endpoint {} has not been started yet. Stashing message {} until processing is started.",
				rpcEndpoint.getClass().getName(),
				message.getClass().getName());
			stash();
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
			} else if (message instanceof WatchOperation) {
				handleWatchMessage((WatchOperation) message);
			} else if (message instanceof Terminated) {
				handleTerminated((Terminated) message);
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
				if (rpcMethod.getReturnType().equals(Void.TYPE)) {
					// No return value to send back
					rpcMethod.invoke(rpcEndpoint, rpcInvocation.getArgs());
				} else {
					Object result = rpcMethod.invoke(rpcEndpoint, rpcInvocation.getArgs());

					if (result instanceof Future) {
						final Future<?> future = (Future<?>) result;

						// pipe result to sender
						if (future instanceof FlinkFuture) {
							// FlinkFutures are currently backed by Scala's futures
							FlinkFuture<?> flinkFuture = (FlinkFuture<?>) future;

							Patterns.pipe(flinkFuture.getScalaFuture(), getContext().dispatcher()).to(getSender());
						} else {
							// We have to unpack the Flink future and pack it into a Scala future
							Patterns.pipe(Futures.future(new Callable<Object>() {
								@Override
								public Object call() throws Exception {
									return future.get();
								}
							}, getContext().dispatcher()), getContext().dispatcher());
						}
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
		else if (runAsync.getDelay() == 0) {
			// run immediately
			try {
				runAsync.getRunnable().run();
			} catch (final Throwable e) {
				LOG.error("Caught exception while executing runnable in main thread.", e);
			}
		}
		else {
			// schedule for later. send a new message after the delay, which will then be immediately executed 
			FiniteDuration delay = new FiniteDuration(runAsync.getDelay(), TimeUnit.MILLISECONDS);
			RunAsync message = new RunAsync(runAsync.getRunnable(), 0);

			getContext().system().scheduler().scheduleOnce(delay, getSelf(), message,
				getContext().dispatcher(), ActorRef.noSender());
		}
	}

	/**
	 * Handle watch operation, add watch or remove watch.
	 *
	 * @param watchMessage watch operation
	 */
	private void handleWatchMessage(final WatchOperation watchMessage) {
		final RpcGateway watchRpcGateway = watchMessage.getRpcGateway();
		// fetch underlying actorRef of watch rpc gateway
		ActorSelection actorSelection = getContext().actorSelection(watchRpcGateway.getAddress());
		scala.concurrent.Future<ActorRef> actorRefFuture = actorSelection.resolveOne(new Timeout(watchMessage.getTimeoutInMillis(), TimeUnit.MILLISECONDS));

		// if success to fetch underlying actorRef, handle watch operation on actorRef
		actorRefFuture.onSuccess(new OnSuccess<ActorRef>() {
			@Override
			public void onSuccess(ActorRef actorRef) throws Throwable {
				if (watchMessage instanceof WatchOperation.AddWatch) {
					RpcGateway existedWatchRpcGateway = watchActorRefs.put(actorRef, watchRpcGateway);
					if (existedWatchRpcGateway == null) {
						getContext().watch(actorRef);
					}
				} else if (watchMessage instanceof WatchOperation.RemoveWatch) {
					RpcGateway existedWatchRpcGateway = watchActorRefs.remove(actorRef);
					if (existedWatchRpcGateway != null) {
						getContext().unwatch(actorRef);
					}
				} else {
					LOG.warn(
						"Received watch operation message of unknown type {} with value {}. Dropping this message!",
						watchMessage.getClass().getName(),
						watchMessage);
				}
			}
		}, getContext().dispatcher());

		// if fail to fetch underlying actoRef, notify rpcEndpoint that the input rpcGateway is unreachable
		actorRefFuture.onFailure(new OnFailure() {
			@Override
			public void onFailure(Throwable failure) throws Throwable {
				LOG.warn(
					"Received watch operation message on unreachable gateway address {} with value {}!",
					watchRpcGateway.getAddress(),
					watchMessage);
				rpcEndpoint.notifyUnreachableRpcGateway(watchRpcGateway);
			}
		}, getContext().dispatcher());
	}

	/**
	 * Handle the terminated message
	 *
	 * @param terminated terminated message which contain dead actorRef
	 */
	private void handleTerminated(Terminated terminated) {
		ActorRef deadActorRef = terminated.getActor();
		RpcGateway existedWatchRpcGateway = watchActorRefs.remove(deadActorRef);
		getContext().unwatch(deadActorRef);
		if (existedWatchRpcGateway != null) {
			rpcEndpoint.notifyUnreachableRpcGateway(existedWatchRpcGateway);
		}
	}

	/**
	 * Look up the rpc method on the given {@link RpcEndpoint} instance.
	 *
	 * @param methodName     Name of the method
	 * @param parameterTypes Parameter types of the method
	 * @return Method of the rpc endpoint
	 * @throws NoSuchMethodException Thrown if the method with the given name and parameter types
	 *                               cannot be found at the rpc endpoint
	 */
	private Method lookupRpcMethod(final String methodName, final Class<?>[] parameterTypes) throws NoSuchMethodException {
		return rpcEndpoint.getClass().getMethod(methodName, parameterTypes);
	}
}
