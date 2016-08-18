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
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rpc.MainThreadExecutor;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.StartStoppable;
import org.apache.flink.runtime.rpc.akka.messages.CallAsync;
import org.apache.flink.runtime.rpc.akka.messages.LocalRpcInvocation;
import org.apache.flink.runtime.rpc.akka.messages.Processing;
import org.apache.flink.runtime.rpc.akka.messages.RemoteRpcInvocation;
import org.apache.flink.runtime.rpc.akka.messages.RpcInvocation;
import org.apache.flink.runtime.rpc.akka.messages.RunAsync;
import org.apache.flink.util.Preconditions;
import org.apache.log4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.BitSet;
import java.util.concurrent.Callable;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Invocation handler to be used with an {@link AkkaRpcActor}. The invocation handler wraps the
 * rpc in a {@link LocalRpcInvocation} message and then sends it to the {@link AkkaRpcActor} where it is
 * executed.
 */
class AkkaInvocationHandler implements InvocationHandler, AkkaGateway, MainThreadExecutor, StartStoppable {
	private static final Logger LOG = Logger.getLogger(AkkaInvocationHandler.class);

	private final String address;

	private final ActorRef rpcEndpoint;

	// whether the actor ref is local and thus no message serialization is needed
	private final boolean isLocal;

	// default timeout for asks
	private final Timeout timeout;

	private final long maximumFramesize;

	AkkaInvocationHandler(String address, ActorRef rpcEndpoint, Timeout timeout, long maximumFramesize) {
		this.address = Preconditions.checkNotNull(address);
		this.rpcEndpoint = Preconditions.checkNotNull(rpcEndpoint);
		this.isLocal = this.rpcEndpoint.path().address().hasLocalScope();
		this.timeout = Preconditions.checkNotNull(timeout);
		this.maximumFramesize = maximumFramesize;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Class<?> declaringClass = method.getDeclaringClass();

		Object result;

		if (declaringClass.equals(AkkaGateway.class) || declaringClass.equals(MainThreadExecutor.class) ||
			declaringClass.equals(Object.class) || declaringClass.equals(StartStoppable.class) ||
			declaringClass.equals(RpcGateway.class)) {
			result = method.invoke(this, args);
		} else {
			String methodName = method.getName();
			Class<?>[] parameterTypes = method.getParameterTypes();
			Annotation[][] parameterAnnotations = method.getParameterAnnotations();
			Timeout futureTimeout = extractRpcTimeout(parameterAnnotations, args, timeout);

			Tuple2<Class<?>[], Object[]> filteredArguments = filterArguments(
				parameterTypes,
				parameterAnnotations,
				args);

			RpcInvocation rpcInvocation;

			if (isLocal) {
				rpcInvocation = new LocalRpcInvocation(
					methodName,
					filteredArguments.f0,
					filteredArguments.f1);
			} else {
				try {
					RemoteRpcInvocation remoteRpcInvocation = new RemoteRpcInvocation(
						methodName,
						filteredArguments.f0,
						filteredArguments.f1);

					if (remoteRpcInvocation.getSize() > maximumFramesize) {
						throw new IOException("The rpc invocation size exceeds the maximum akka framesize.");
					} else {
						rpcInvocation = remoteRpcInvocation;
					}
				} catch (IOException e) {
					LOG.warn("Could not create remote rpc invocation message. Failing rpc invocation because...", e);
					throw e;
				}
			}

			Class<?> returnType = method.getReturnType();

			if (returnType.equals(Void.TYPE)) {
				rpcEndpoint.tell(rpcInvocation, ActorRef.noSender());

				result = null;
			} else if (returnType.equals(Future.class)) {
				// execute an asynchronous call
				result = Patterns.ask(rpcEndpoint, rpcInvocation, futureTimeout);
			} else {
				// execute a synchronous call
				Future<?> futureResult = Patterns.ask(rpcEndpoint, rpcInvocation, futureTimeout);
				FiniteDuration duration = timeout.duration();

				result = Await.result(futureResult, duration);
			}
		}

		return result;
	}

	@Override
	public ActorRef getRpcEndpoint() {
		return rpcEndpoint;
	}

	@Override
	public void runAsync(Runnable runnable) {
		scheduleRunAsync(runnable, 0);
	}

	@Override
	public void scheduleRunAsync(Runnable runnable, long delay) {
		checkNotNull(runnable, "runnable");
		checkArgument(delay >= 0, "delay must be zero or greater");
		
		if (isLocal) {
			rpcEndpoint.tell(new RunAsync(runnable, delay), ActorRef.noSender());
		} else {
			throw new RuntimeException("Trying to send a Runnable to a remote actor at " +
				rpcEndpoint.path() + ". This is not supported.");
		}
	}

	@Override
	public <V> Future<V> callAsync(Callable<V> callable, Timeout callTimeout) {
		if(isLocal) {
			@SuppressWarnings("unchecked")
			Future<V> result = (Future<V>) Patterns.ask(rpcEndpoint, new CallAsync(callable), callTimeout);

			return result;
		} else {
			throw new RuntimeException("Trying to send a Callable to a remote actor at " +
				rpcEndpoint.path() + ". This is not supported.");
		}
	}

	@Override
	public void start() {
		rpcEndpoint.tell(Processing.START, ActorRef.noSender());
	}

	@Override
	public void stop() {
		rpcEndpoint.tell(Processing.STOP, ActorRef.noSender());
	}

	// ------------------------------------------------------------------------
	//  Helper methods
	// ------------------------------------------------------------------------

	/**
	 * Extracts the {@link RpcTimeout} annotated rpc timeout value from the list of given method
	 * arguments. If no {@link RpcTimeout} annotated parameter could be found, then the default
	 * timeout is returned.
	 *
	 * @param parameterAnnotations Parameter annotations
	 * @param args Array of arguments
	 * @param defaultTimeout Default timeout to return if no {@link RpcTimeout} annotated parameter
	 *                       has been found
	 * @return Timeout extracted from the array of arguments or the default timeout
	 */
	private static Timeout extractRpcTimeout(Annotation[][] parameterAnnotations, Object[] args, Timeout defaultTimeout) {
		if (args != null) {
			Preconditions.checkArgument(parameterAnnotations.length == args.length);

			for (int i = 0; i < parameterAnnotations.length; i++) {
				if (isRpcTimeout(parameterAnnotations[i])) {
					if (args[i] instanceof FiniteDuration) {
						return new Timeout((FiniteDuration) args[i]);
					} else {
						throw new RuntimeException("The rpc timeout parameter must be of type " +
							FiniteDuration.class.getName() + ". The type " + args[i].getClass().getName() +
							" is not supported.");
					}
				}
			}
		}

		return defaultTimeout;
	}

	/**
	 * Removes all {@link RpcTimeout} annotated parameters from the parameter type and argument
	 * list.
	 *
	 * @param parameterTypes Array of parameter types
	 * @param parameterAnnotations Array of parameter annotations
	 * @param args Arary of arguments
	 * @return Tuple of filtered parameter types and arguments which no longer contain the
	 * {@link RpcTimeout} annotated parameter types and arguments
	 */
	private static Tuple2<Class<?>[], Object[]> filterArguments(
		Class<?>[] parameterTypes,
		Annotation[][] parameterAnnotations,
		Object[] args) {

		Class<?>[] filteredParameterTypes;
		Object[] filteredArgs;

		if (args == null) {
			filteredParameterTypes = parameterTypes;
			filteredArgs = null;
		} else {
			Preconditions.checkArgument(parameterTypes.length == parameterAnnotations.length);
			Preconditions.checkArgument(parameterAnnotations.length == args.length);

			BitSet isRpcTimeoutParameter = new BitSet(parameterTypes.length);
			int numberRpcParameters = parameterTypes.length;

			for (int i = 0; i < parameterTypes.length; i++) {
				if (isRpcTimeout(parameterAnnotations[i])) {
					isRpcTimeoutParameter.set(i);
					numberRpcParameters--;
				}
			}

			if (numberRpcParameters == parameterTypes.length) {
				filteredParameterTypes = parameterTypes;
				filteredArgs = args;
			} else {
				filteredParameterTypes = new Class<?>[numberRpcParameters];
				filteredArgs = new Object[numberRpcParameters];
				int counter = 0;

				for (int i = 0; i < parameterTypes.length; i++) {
					if (!isRpcTimeoutParameter.get(i)) {
						filteredParameterTypes[counter] = parameterTypes[i];
						filteredArgs[counter] = args[i];
						counter++;
					}
				}
			}
		}

		return Tuple2.of(filteredParameterTypes, filteredArgs);
	}

	/**
	 * Checks whether any of the annotations is of type {@link RpcTimeout}
	 *
	 * @param annotations Array of annotations
	 * @return True if {@link RpcTimeout} was found; otherwise false
	 */
	private static boolean isRpcTimeout(Annotation[] annotations) {
		for (Annotation annotation : annotations) {
			if (annotation.annotationType().equals(RpcTimeout.class)) {
				return true;
			}
		}

		return false;
	}

	@Override
	public String getAddress() {
		return address;
	}
}
