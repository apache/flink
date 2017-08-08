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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rpc.MainThreadExecutable;
import org.apache.flink.runtime.rpc.RpcServer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Invocation handler to be used with an {@link AkkaRpcActor}. The invocation handler wraps the
 * rpc in a {@link LocalRpcInvocation} message and then sends it to the {@link AkkaRpcActor} where it is
 * executed.
 */
class AkkaInvocationHandler implements InvocationHandler, AkkaGateway, RpcServer {
	private static final Logger LOG = LoggerFactory.getLogger(AkkaInvocationHandler.class);

	/**
	 * The Akka (RPC) address of {@link #rpcEndpoint} including host and port of the ActorSystem in
	 * which the actor is running.
	 */
	private final String address;

	/**
	 * Hostname of the host, {@link #rpcEndpoint} is running on.
	 */
	private final String hostname;

	private final ActorRef rpcEndpoint;

	// whether the actor ref is local and thus no message serialization is needed
	private final boolean isLocal;

	// default timeout for asks
	private final Time timeout;

	private final long maximumFramesize;

	// null if gateway; otherwise non-null
	private final CompletableFuture<Void> terminationFuture;

	AkkaInvocationHandler(
			String address,
			String hostname,
			ActorRef rpcEndpoint,
			Time timeout,
			long maximumFramesize,
			@Nullable CompletableFuture<Void> terminationFuture) {

		this.address = Preconditions.checkNotNull(address);
		this.hostname = Preconditions.checkNotNull(hostname);
		this.rpcEndpoint = Preconditions.checkNotNull(rpcEndpoint);
		this.isLocal = this.rpcEndpoint.path().address().hasLocalScope();
		this.timeout = Preconditions.checkNotNull(timeout);
		this.maximumFramesize = maximumFramesize;
		this.terminationFuture = terminationFuture;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Class<?> declaringClass = method.getDeclaringClass();

		Object result;

		if (declaringClass.equals(AkkaGateway.class) ||
			declaringClass.equals(Object.class) ||
			declaringClass.equals(RpcGateway.class) ||
			declaringClass.equals(StartStoppable.class) ||
			declaringClass.equals(MainThreadExecutable.class) ||
			declaringClass.equals(RpcServer.class)) {
			result = method.invoke(this, args);
		} else {
			String methodName = method.getName();
			Class<?>[] parameterTypes = method.getParameterTypes();
			Annotation[][] parameterAnnotations = method.getParameterAnnotations();
			Time futureTimeout = extractRpcTimeout(parameterAnnotations, args, timeout);

			RpcInvocation rpcInvocation;

			if (isLocal) {
				rpcInvocation = new LocalRpcInvocation(
					methodName,
					parameterTypes,
					args);
			} else {
				try {
					RemoteRpcInvocation remoteRpcInvocation = new RemoteRpcInvocation(
						methodName,
						parameterTypes,
						args);

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

			if (Objects.equals(returnType, Void.TYPE)) {
				rpcEndpoint.tell(rpcInvocation, ActorRef.noSender());

				result = null;
			} else if (Objects.equals(returnType,CompletableFuture.class)) {
				// execute an asynchronous call
				result = FutureUtils.toJava(Patterns.ask(rpcEndpoint, rpcInvocation, futureTimeout.toMilliseconds()));
			} else {
				// execute a synchronous call
				CompletableFuture<?> futureResult = FutureUtils.toJava(
					Patterns.ask(rpcEndpoint, rpcInvocation, futureTimeout.toMilliseconds()));

				result = futureResult.get(futureTimeout.getSize(), futureTimeout.getUnit());
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
	public void scheduleRunAsync(Runnable runnable, long delayMillis) {
		checkNotNull(runnable, "runnable");
		checkArgument(delayMillis >= 0, "delay must be zero or greater");

		if (isLocal) {
			long atTimeNanos = delayMillis == 0 ? 0 : System.nanoTime() + (delayMillis * 1_000_000);
			rpcEndpoint.tell(new RunAsync(runnable, atTimeNanos), ActorRef.noSender());
		} else {
			throw new RuntimeException("Trying to send a Runnable to a remote actor at " +
				rpcEndpoint.path() + ". This is not supported.");
		}
	}

	@Override
	public <V> CompletableFuture<V> callAsync(Callable<V> callable, Time callTimeout) {
		if(isLocal) {
			@SuppressWarnings("unchecked")
			scala.concurrent.Future<V> resultFuture = (scala.concurrent.Future<V>) Patterns.ask(rpcEndpoint, new CallAsync(callable), callTimeout.toMilliseconds());

			return FutureUtils.toJava(resultFuture);
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
	private static Time extractRpcTimeout(Annotation[][] parameterAnnotations, Object[] args, Time defaultTimeout) {
		if (args != null) {
			Preconditions.checkArgument(parameterAnnotations.length == args.length);

			for (int i = 0; i < parameterAnnotations.length; i++) {
				if (isRpcTimeout(parameterAnnotations[i])) {
					if (args[i] instanceof Time) {
						return (Time) args[i];
					} else {
						throw new RuntimeException("The rpc timeout parameter must be of type " +
							Time.class.getName() + ". The type " + args[i].getClass().getName() +
							" is not supported.");
					}
				}
			}
		}

		return defaultTimeout;
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

	@Override
	public String getHostname() {
		return hostname;
	}

	@Override
	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}
}
