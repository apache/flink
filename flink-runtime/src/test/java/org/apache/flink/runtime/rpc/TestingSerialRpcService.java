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

import akka.dispatch.ExecutionContexts;
import akka.dispatch.Futures;
import akka.util.Timeout;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;
import org.apache.log4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.BitSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class TestingSerialRpcService implements RpcService {
	private final ScheduledExecutorService executorService;
	private final ConcurrentHashMap<String, RpcGateway> registeredConnections;

	public TestingSerialRpcService() {
		executorService = Executors.newSingleThreadScheduledExecutor();
		this.registeredConnections = new ConcurrentHashMap<>();
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

	@Override
	public <C extends RpcGateway> Future<C> connect(String address, Class<C> clazz) {
		RpcGateway gateway = registeredConnections.get(address);

		if (gateway != null) {
			if (clazz.isAssignableFrom(gateway.getClass())) {
				@SuppressWarnings("unchecked")
				C typedGateway = (C) gateway;
				return Futures.successful(typedGateway);
			} else {
				return Futures.failed(
					new Exception("Gateway registered under " + address + " is not of type " + clazz));
			}
		} else {
			return Futures.failed(new Exception("No gateway registered under that name"));
		}
	}

	@Override
	public void scheduleRunnable(Runnable runnable, long delay, TimeUnit unit) {
		executorService.schedule(runnable, delay, unit);
	}

	@Override
	public ExecutionContext getExecutionContext() {
		return ExecutionContexts.fromExecutorService(executorService);
	}

	@Override
	public void stopService() {
		executorService.shutdown();
		registeredConnections.clear();
	}

	@Override
	public <C extends RpcGateway> void stopServer(C selfGateway) {

	}

	@Override
	public <C extends RpcGateway, S extends RpcEndpoint<C>> C startServer(S rpcEndpoint) {
		InvocationHandler akkaInvocationHandler = new TestingSerialInvocationHandler(rpcEndpoint, executorService);

		ClassLoader classLoader = getClass().getClassLoader();

		@SuppressWarnings("unchecked")
		C self = (C) Proxy.newProxyInstance(
			classLoader,
			new Class<?>[]{
				rpcEndpoint.getSelfGatewayType(),
				MainThreadExecutor.class,
				StartStoppable.class,
				RpcGateway.class},
			akkaInvocationHandler);

		return self;
	}

	static class TestingSerialInvocationHandler<C extends RpcGateway, T extends RpcEndpoint<C>> implements InvocationHandler, RpcGateway, MainThreadExecutor, StartStoppable {
		private static final Logger LOG = Logger.getLogger(TestingSerialInvocationHandler.class);

		private final T rpcEndpoint;
		private final ScheduledExecutorService executorService;
		private final Timeout timeout;

		public TestingSerialInvocationHandler(T rpcEndpoint, ScheduledExecutorService executorService) {
			this(rpcEndpoint, executorService, new Timeout(new FiniteDuration(10, TimeUnit.SECONDS)));
		}

		public TestingSerialInvocationHandler(T rpcEndpoint, ScheduledExecutorService executorService, Timeout timeout) {
			this.rpcEndpoint = rpcEndpoint;
			this.executorService = executorService;
			this.timeout = timeout;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			Class<?> declaringClass = method.getDeclaringClass();
			if (declaringClass.equals(MainThreadExecutor.class) ||
				declaringClass.equals(Object.class) || declaringClass.equals(StartStoppable.class) ||
				declaringClass.equals(RpcGateway.class)) {
				return method.invoke(this, args);
			} else {
				final String methodName = method.getName();
				Class<?>[] parameterTypes = method.getParameterTypes();
				Annotation[][] parameterAnnotations = method.getParameterAnnotations();
				Timeout futureTimeout = extractRpcTimeout(parameterAnnotations, args, timeout);

				final Tuple2<Class<?>[], Object[]> filteredArguments = filterArguments(
					parameterTypes,
					parameterAnnotations,
					args);

				Class<?> returnType = method.getReturnType();

				if (returnType.equals(Void.TYPE)) {
					return handleRpcInvocationSync(methodName, filteredArguments.f0, filteredArguments.f1, futureTimeout);
				} else if (returnType.equals(Future.class)) {
					try {
						Object result = handleRpcInvocationSync(methodName, filteredArguments.f0, filteredArguments.f1, futureTimeout);
						return Futures.successful(result);
					} catch(Throwable e) {
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
		 *
		 */
		private Object handleRpcInvocationSync(final String methodName,
			final Class<?>[] parameterTypes,
			final Object[] args,
			final Timeout futureTimeout) throws Exception {
			final Method rpcMethod = lookupRpcMethod(methodName, parameterTypes);
			ScheduledFuture<Object> scheduleFuture = executorService.schedule(new Callable<Object>() {
				@Override
				public Object call() {
					try {
						return rpcMethod.invoke(rpcEndpoint, args);
					} catch(Throwable e) {
						throw new RuntimeException(e);
					}
				}
			}, 0, TimeUnit.MILLISECONDS);

			Class<?> returnType = rpcMethod.getReturnType();
			if(returnType.equals(Void.TYPE)) {
				scheduleFuture.get(futureTimeout.duration().toMillis(), TimeUnit.MILLISECONDS);
				return null;
			} else if(returnType.equals(Future.class)){
				Future<?> futureResult = (Future<?>)scheduleFuture.get();
				return Await.result(futureResult, futureTimeout.duration());
			} else {
				return scheduleFuture.get(futureTimeout.duration().toMillis(), TimeUnit.MILLISECONDS);
			}
		}

		@Override
		public void runAsync(Runnable runnable) {
			executorService.execute(runnable);
		}

		@Override
		public <V> Future<V> callAsync(Callable<V> callable, Timeout callTimeout) {
			ScheduledFuture<V> future =  executorService.schedule(callable, 0, TimeUnit.MILLISECONDS);
			try{
				V result = future.get(callTimeout.duration().toMillis(), TimeUnit.MILLISECONDS);
				return Futures.successful(result);
			} catch(Throwable e) {
				return Futures.failed(e);
			}
		}

		@Override
		public void scheduleRunAsync(Runnable runnable, long delay) {
			executorService.schedule(runnable, delay, TimeUnit.MILLISECONDS);
		}

		@Override
		public void start() {

		}

		@Override
		public void stop() {

		}

		@Override
		public String getAddress() {
			return null;
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

	}
}
