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

package org.apache.flink.runtime.rpc;

import akka.dispatch.Futures;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.util.DirectExecutorService;
import org.apache.flink.util.Preconditions;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.BitSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * An RPC Service implementation for testing. This RPC service directly executes all asynchronous
 * calls one by one in the calling thread.
 */
public class TestingSerialRpcService implements RpcService {

	private final DirectExecutorService executorService;
	private final ConcurrentHashMap<String, RpcGateway> registeredConnections;

	public TestingSerialRpcService() {
		executorService = new DirectExecutorService();
		this.registeredConnections = new ConcurrentHashMap<>(16);
	}

	@Override
	public void scheduleRunnable(final Runnable runnable, final long delay, final TimeUnit unit) {
		try {
			unit.sleep(delay);
			runnable.run();
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void execute(Runnable runnable) {
		runnable.run();
	}

	@Override
	public <T> Future<T> execute(Callable<T> callable) {
		try {
			T result = callable.call();

			return FlinkCompletableFuture.completed(result);
		} catch (Exception e) {
			return FlinkCompletableFuture.completedExceptionally(e);
		}
	}

	@Override
	public Executor getExecutor() {
		return executorService;
	}

	@Override
	public void stopService() {
		executorService.shutdown();
		registeredConnections.clear();
	}

	@Override
	public void stopServer(RpcGateway selfGateway) {
		registeredConnections.remove(selfGateway.getAddress());
	}

	@Override
	public <C extends RpcGateway, S extends RpcEndpoint<C>> C startServer(S rpcEndpoint) {
		final String address = UUID.randomUUID().toString();

		InvocationHandler akkaInvocationHandler = new TestingSerialRpcService.TestingSerialInvocationHandler<>(address, rpcEndpoint);
		ClassLoader classLoader = getClass().getClassLoader();

		@SuppressWarnings("unchecked")
		C self = (C) Proxy.newProxyInstance(
			classLoader,
			new Class<?>[]{
				rpcEndpoint.getSelfGatewayType(),
				MainThreadExecutable.class,
				StartStoppable.class,
				RpcGateway.class
			},
			akkaInvocationHandler);

		// register self
		registeredConnections.putIfAbsent(self.getAddress(), self);

		return self;
	}

	@Override
	public String getAddress() {
		return "";
	}

	@Override
	public <C extends RpcGateway> Future<C> connect(String address, Class<C> clazz) {
		RpcGateway gateway = registeredConnections.get(address);

		if (gateway != null) {
			if (clazz.isAssignableFrom(gateway.getClass())) {
				@SuppressWarnings("unchecked")
				C typedGateway = (C) gateway;
				return FlinkCompletableFuture.completed(typedGateway);
			} else {
				return FlinkCompletableFuture.completedExceptionally(
					new Exception("Gateway registered under " + address + " is not of type " + clazz));
			}
		} else {
			return FlinkCompletableFuture.completedExceptionally(new Exception("No gateway registered under that name"));
		}
	}

	// ------------------------------------------------------------------------
	// connections
	// ------------------------------------------------------------------------

	public void registerGateway(String address, RpcGateway gateway) {
		checkNotNull(address);
		checkNotNull(gateway);

		if (registeredConnections.putIfAbsent(address, gateway) != null) {
			throw new IllegalStateException("a gateway is already registered under " + address);
		}
	}

	public void clearGateways() {
		registeredConnections.clear();
	}

	private static final class TestingSerialInvocationHandler<C extends RpcGateway, T extends RpcEndpoint<C>> implements InvocationHandler, RpcGateway, MainThreadExecutable, StartStoppable {

		private final T rpcEndpoint;

		/** default timeout for asks */
		private final Time timeout;

		private final String address;

		private TestingSerialInvocationHandler(String address, T rpcEndpoint) {
			this(address, rpcEndpoint, Time.seconds(10));
		}

		private TestingSerialInvocationHandler(String address, T rpcEndpoint, Time timeout) {
			this.rpcEndpoint = rpcEndpoint;
			this.timeout = timeout;
			this.address = address;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			Class<?> declaringClass = method.getDeclaringClass();
			if (declaringClass.equals(MainThreadExecutable.class) ||
				declaringClass.equals(Object.class) || declaringClass.equals(StartStoppable.class) ||
				declaringClass.equals(RpcGateway.class)) {
				return method.invoke(this, args);
			} else {
				final String methodName = method.getName();
				Class<?>[] parameterTypes = method.getParameterTypes();
				Annotation[][] parameterAnnotations = method.getParameterAnnotations();
				Time futureTimeout = extractRpcTimeout(parameterAnnotations, args, timeout);

				final Tuple2<Class<?>[], Object[]> filteredArguments = filterArguments(
					parameterTypes,
					parameterAnnotations,
					args);

				Class<?> returnType = method.getReturnType();

				if (returnType.equals(Future.class)) {
					try {
						Object result = handleRpcInvocationSync(methodName, filteredArguments.f0, filteredArguments.f1, futureTimeout);
						return Futures.successful(result);
					} catch (Throwable e) {
						return Futures.failed(e);
					}
				} else {
					return handleRpcInvocationSync(methodName, filteredArguments.f0, filteredArguments.f1, futureTimeout);
				}
			}
		}

		/**
		 * Handle rpc invocations by looking up the rpc method on the rpc endpoint and calling this
		 * method with the provided method arguments. If the method has a return value, it is returned
		 * to the sender of the call.
		 */
		private Object handleRpcInvocationSync(final String methodName,
			final Class<?>[] parameterTypes,
			final Object[] args,
			final Time futureTimeout) throws Exception {
			final Method rpcMethod = lookupRpcMethod(methodName, parameterTypes);
			Object result = rpcMethod.invoke(rpcEndpoint, args);

			if (result instanceof Future) {
				Future<?> future = (Future<?>) result;
				return future.get(futureTimeout.getSize(), futureTimeout.getUnit());
			} else {
				return result;
			}
		}

		@Override
		public void runAsync(Runnable runnable) {
			runnable.run();
		}

		@Override
		public <V> Future<V> callAsync(Callable<V> callable, Time callTimeout) {
			try {
				return FlinkCompletableFuture.completed(callable.call());
			} catch (Throwable e) {
				return FlinkCompletableFuture.completedExceptionally(e);
			}
		}

		@Override
		public void scheduleRunAsync(final Runnable runnable, final long delay) {
			try {
				TimeUnit.MILLISECONDS.sleep(delay);
				runnable.run();
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public String getAddress() {
			return address;
		}

		@Override
		public void start() {
			// do nothing
		}

		@Override
		public void stop() {
			// do nothing
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
		private Method lookupRpcMethod(final String methodName,
			final Class<?>[] parameterTypes) throws NoSuchMethodException {
			return rpcEndpoint.getClass().getMethod(methodName, parameterTypes);
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
		 * @param args                 Array of arguments
		 * @param defaultTimeout       Default timeout to return if no {@link RpcTimeout} annotated parameter
		 *                             has been found
		 * @return Timeout extracted from the array of arguments or the default timeout
		 */
		private static Time extractRpcTimeout(Annotation[][] parameterAnnotations, Object[] args,
			Time defaultTimeout) {
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
		 * Removes all {@link RpcTimeout} annotated parameters from the parameter type and argument
		 * list.
		 *
		 * @param parameterTypes       Array of parameter types
		 * @param parameterAnnotations Array of parameter annotations
		 * @param args                 Arary of arguments
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
}
