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
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.akka.messages.CallAsync;
import org.apache.flink.runtime.rpc.akka.messages.RpcInvocation;
import org.apache.flink.runtime.rpc.akka.messages.RunAsync;
import org.apache.flink.util.Preconditions;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.BitSet;
import java.util.concurrent.Callable;

/**
 * Invocation handler to be used with a {@link AkkaRpcActor}. The invocation handler wraps the
 * rpc in a {@link RpcInvocation} message and then sends it to the {@link AkkaRpcActor} where it is
 * executed.
 */
class AkkaInvocationHandler implements InvocationHandler, AkkaGateway, MainThreadExecutor {
	private final ActorRef rpcServer;

	// default timeout for asks
	private final Timeout timeout;

	AkkaInvocationHandler(ActorRef rpcServer, Timeout timeout) {
		this.rpcServer = Preconditions.checkNotNull(rpcServer);
		this.timeout = Preconditions.checkNotNull(timeout);
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Class<?> declaringClass = method.getDeclaringClass();

		Object result;

		if (declaringClass.equals(AkkaGateway.class) || declaringClass.equals(MainThreadExecutor.class) || declaringClass.equals(Object.class)) {
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

			RpcInvocation rpcInvocation = new RpcInvocation(
				methodName,
				filteredArguments.f0,
				filteredArguments.f1);

			Class<?> returnType = method.getReturnType();

			if (returnType.equals(Void.TYPE)) {
				rpcServer.tell(rpcInvocation, ActorRef.noSender());

				result = null;
			} else if (returnType.equals(Future.class)) {
				// execute an asynchronous call
				result = Patterns.ask(rpcServer, rpcInvocation, futureTimeout);
			} else {
				// execute a synchronous call
				Future<?> futureResult = Patterns.ask(rpcServer, rpcInvocation, futureTimeout);
				FiniteDuration duration = timeout.duration();

				result = Await.result(futureResult, duration);
			}
		}

		return result;
	}

	@Override
	public ActorRef getRpcServer() {
		return rpcServer;
	}

	@Override
	public void runAsync(Runnable runnable) {
		// Unfortunately I couldn't find a way to allow only local communication. Therefore, the
		// runnable field is transient transient
		rpcServer.tell(new RunAsync(runnable), ActorRef.noSender());
	}

	@Override
	public <V> Future<V> callAsync(Callable<V> callable, Timeout callTimeout) {
		// Unfortunately I couldn't find a way to allow only local communication. Therefore, the
		// callable field is declared transient
		@SuppressWarnings("unchecked")
		Future<V> result = (Future<V>) Patterns.ask(rpcServer, new CallAsync(callable), callTimeout);

		return result;
	}

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
}
