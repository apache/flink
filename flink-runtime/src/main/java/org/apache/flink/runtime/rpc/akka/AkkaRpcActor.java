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

import akka.actor.Status;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.akka.messages.CallAsync;
import org.apache.flink.runtime.rpc.akka.messages.RpcInvocation;
import org.apache.flink.runtime.rpc.akka.messages.RunAsync;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

/**
 * Akka rpc actor which receives {@link RpcInvocation}, {@link RunAsync} and {@link CallAsync}
 * messages.
 * <p>
 * The {@link RpcInvocation} designates a rpc and is dispatched to the given {@link RpcEndpoint}
 * instance.
 * <p>
 * The {@link RunAsync} and {@link CallAsync} messages contain executable code which is executed
 * in the context of the actor thread.
 *
 * @param <C> Type of the {@link RpcGateway} associated with the {@link RpcEndpoint}
 * @param <T> Type of the {@link RpcEndpoint}
 */
class AkkaRpcActor<C extends RpcGateway, T extends RpcEndpoint<C>> extends UntypedActor {
	private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcActor.class);

	private final T rpcEndpoint;

	AkkaRpcActor(final T rpcEndpoint) {
		this.rpcEndpoint = Preconditions.checkNotNull(rpcEndpoint, "rpc endpoint");
	}

	@Override
	public void onReceive(final Object message)  {
		if (message instanceof RunAsync) {
			handleRunAsync((RunAsync) message);
		} else if (message instanceof CallAsync) {
			handleCallAsync((CallAsync) message);
		} else if (message instanceof RpcInvocation) {
			handleRpcInvocation((RpcInvocation) message);
		} else {
			LOG.warn("Received message of unknown type {}. Dropping this message!", message.getClass());
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
			rpcMethod = lookupRpcMethod(rpcInvocation.getMethodName(), rpcInvocation.getParameterTypes());
		} catch (final NoSuchMethodException e) {
			LOG.error("Could not find rpc method for rpc invocation: {}.", rpcInvocation, e);
		}

		if (rpcMethod != null) {
			if (rpcMethod.getReturnType().equals(Void.TYPE)) {
				// No return value to send back
				try {
					rpcMethod.invoke(rpcEndpoint, rpcInvocation.getArgs());
				} catch (Throwable e) {
					LOG.error("Error while executing remote procedure call {}.", rpcMethod, e);
				}
			} else {
				try {
					Object result = rpcMethod.invoke(rpcEndpoint, rpcInvocation.getArgs());

					if (result instanceof Future) {
						// pipe result to sender
						Patterns.pipe((Future<?>) result, getContext().dispatcher()).to(getSender());
					} else {
						// tell the sender the result of the computation
						getSender().tell(new Status.Success(result), getSelf());
					}
				} catch (Throwable e) {
					// tell the sender about the failure
					getSender().tell(new Status.Failure(e), getSelf());
				}
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
		} else {
			try {
				runAsync.getRunnable().run();
			} catch (final Throwable e) {
				LOG.error("Caught exception while executing runnable in main thread.", e);
			}
		}
	}

	/**
	 * Look up the rpc method on the given {@link RpcEndpoint} instance.
	 *
	 * @param methodName Name of the method
	 * @param parameterTypes Parameter types of the method
	 * @return Method of the rpc endpoint
	 * @throws NoSuchMethodException
	 */
	private Method lookupRpcMethod(final String methodName, final Class<?>[] parameterTypes) throws NoSuchMethodException {
		return rpcEndpoint.getClass().getMethod(methodName, parameterTypes);
	}
}
