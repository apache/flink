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

import org.apache.flink.util.ReflectionUtil;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.reflections.Reflections;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RpcCompletenessTest extends TestLogger {
	private static final Class<?> futureClass = Future.class;

	@Test
	public void testRpcCompleteness() {
		Reflections reflections = new Reflections("org.apache.flink");

		Set<Class<? extends RpcEndpoint>> classes = reflections.getSubTypesOf(RpcEndpoint.class);

		Class<? extends RpcEndpoint> c;

		for (Class<? extends RpcEndpoint> rpcEndpoint :classes){
			c = rpcEndpoint;

			Class<?> rpcGatewayType = ReflectionUtil.getTemplateType1(c);

			if (rpcGatewayType != null) {
				checkCompleteness(rpcEndpoint, (Class<? extends RpcGateway>) rpcGatewayType);
			} else {
				fail("Could not retrieve the rpc gateway class for the given rpc endpoint class " + rpcEndpoint.getName());
			}
		}
	}

	private void checkCompleteness(Class<? extends RpcEndpoint> rpcEndpoint, Class<? extends RpcGateway> rpcGateway) {
		Method[] gatewayMethods = rpcGateway.getDeclaredMethods();
		Method[] serverMethods = rpcEndpoint.getDeclaredMethods();

		Map<String, Set<Method>> rpcMethods = new HashMap<>();
		Set<Method> unmatchedRpcMethods = new HashSet<>();

		for (Method serverMethod : serverMethods) {
			if (serverMethod.isAnnotationPresent(RpcMethod.class)) {
				if (rpcMethods.containsKey(serverMethod.getName())) {
					Set<Method> methods = rpcMethods.get(serverMethod.getName());
					methods.add(serverMethod);

					rpcMethods.put(serverMethod.getName(), methods);
				} else {
					Set<Method> methods = new HashSet<>();
					methods.add(serverMethod);

					rpcMethods.put(serverMethod.getName(), methods);
				}

				unmatchedRpcMethods.add(serverMethod);
			}
		}

		for (Method gatewayMethod : gatewayMethods) {
			assertTrue(
				"The rpc endpoint " + rpcEndpoint.getName() + " does not contain a RpcMethod " +
					"annotated method with the same name and signature " +
					generateEndpointMethodSignature(gatewayMethod) + ".",
				rpcMethods.containsKey(gatewayMethod.getName()));

			checkGatewayMethod(gatewayMethod);

			if (!matchGatewayMethodWithEndpoint(gatewayMethod, rpcMethods.get(gatewayMethod.getName()), unmatchedRpcMethods)) {
				fail("Could not find a RpcMethod annotated method in rpc endpoint " +
					rpcEndpoint.getName() + " matching the rpc gateway method " +
					generateEndpointMethodSignature(gatewayMethod) + " defined in the rpc gateway " +
					rpcGateway.getName() + ".");
			}
		}

		if (!unmatchedRpcMethods.isEmpty()) {
			StringBuilder builder = new StringBuilder();

			for (Method unmatchedRpcMethod : unmatchedRpcMethods) {
				builder.append(unmatchedRpcMethod).append("\n");
			}

			fail("The rpc endpoint " + rpcEndpoint.getName() + " contains rpc methods which " +
				"are not matched to gateway methods of " + rpcGateway.getName() + ":\n" +
				builder.toString());
		}
	}

	/**
	 * Checks whether the gateway method fulfills the gateway method requirements.
	 * <ul>
	 *     <li>It checks whether the return type is void or a {@link Future} wrapping the actual result. </li>
	 *     <li>It checks that the method's parameter list contains at most one parameter annotated with {@link RpcTimeout}.</li>
	 * </ul>
	 *
	 * @param gatewayMethod Gateway method to check
	 */
	private void checkGatewayMethod(Method gatewayMethod) {
		if (!gatewayMethod.getReturnType().equals(Void.TYPE)) {
			assertTrue(
				"The return type of method " + gatewayMethod.getName() + " in the rpc gateway " +
					gatewayMethod.getDeclaringClass().getName() + " is non void and not a " +
					"future. Non-void return types have to be returned as a future.",
				gatewayMethod.getReturnType().equals(futureClass));
		}

		Annotation[][] parameterAnnotations = gatewayMethod.getParameterAnnotations();
		Class<?>[] parameterTypes = gatewayMethod.getParameterTypes();
		int rpcTimeoutParameters = 0;

		for (int i = 0; i < parameterAnnotations.length; i++) {
			if (isRpcTimeout(parameterAnnotations[i])) {
				assertTrue(
					"The rpc timeout has to be of type " + FiniteDuration.class.getName() + ".",
					parameterTypes[i].equals(FiniteDuration.class));

				rpcTimeoutParameters++;
			}
		}

		assertTrue("The gateway method " + gatewayMethod + " must have at most one RpcTimeout " +
			"annotated parameter.", rpcTimeoutParameters <= 1);
	}

	/**
	 * Checks whether we find a matching overloaded version for the gateway method among the methods
	 * with the same name in the rpc endpoint.
	 *
	 * @param gatewayMethod Gateway method
	 * @param endpointMethods Set of rpc methods on the rpc endpoint with the same name as the gateway
	 *                   method
	 * @param unmatchedRpcMethods Set of unmatched rpc methods on the endpoint side (so far)
	 */
	private boolean matchGatewayMethodWithEndpoint(Method gatewayMethod, Set<Method> endpointMethods, Set<Method> unmatchedRpcMethods) {
		for (Method endpointMethod : endpointMethods) {
			if (checkMethod(gatewayMethod, endpointMethod)) {
				unmatchedRpcMethods.remove(endpointMethod);
				return true;
			}
		}

		return false;
	}

	private boolean checkMethod(Method gatewayMethod, Method endpointMethod) {
		Class<?>[] gatewayParameterTypes = gatewayMethod.getParameterTypes();
		Annotation[][] gatewayParameterAnnotations = gatewayMethod.getParameterAnnotations();

		Class<?>[] endpointParameterTypes = endpointMethod.getParameterTypes();

		List<Class<?>> filteredGatewayParameterTypes = new ArrayList<>();

		assertEquals(gatewayParameterTypes.length, gatewayParameterAnnotations.length);

		// filter out the RpcTimeout parameters
		for (int i = 0; i < gatewayParameterTypes.length; i++) {
			if (!isRpcTimeout(gatewayParameterAnnotations[i])) {
				filteredGatewayParameterTypes.add(gatewayParameterTypes[i]);
			}
		}

		if (filteredGatewayParameterTypes.size() != endpointParameterTypes.length) {
			return false;
		} else {
			// check the parameter types
			for (int i = 0; i < filteredGatewayParameterTypes.size(); i++) {
				if (!checkType(filteredGatewayParameterTypes.get(i), endpointParameterTypes[i])) {
					return false;
				}
			}

			// check the return types
			if (endpointMethod.getReturnType() == void.class) {
				if (gatewayMethod.getReturnType() != void.class) {
					return false;
				}
			} else {
				// has return value. The gateway method should be wrapped in a future
				Class<?> futureClass = gatewayMethod.getReturnType();

				// sanity check that the return type of a gateway method must be void or a future
				if (!futureClass.equals(RpcCompletenessTest.futureClass)) {
					return false;
				} else {
					Class<?> valueClass = ReflectionUtil.getTemplateType1(gatewayMethod.getGenericReturnType());

					if (endpointMethod.getReturnType().equals(futureClass)) {
						Class<?> rpcEndpointValueClass = ReflectionUtil.getTemplateType1(endpointMethod.getGenericReturnType());

						// check if we have the same future value types
						if (valueClass != null && rpcEndpointValueClass != null && !checkType(valueClass, rpcEndpointValueClass)) {
							return false;
						}
					} else {
						if (valueClass != null && !checkType(valueClass, endpointMethod.getReturnType())) {
							return false;
						}
					}
				}
			}

			return gatewayMethod.getName().equals(endpointMethod.getName());
		}
	}

	private boolean checkType(Class<?> firstType, Class<?> secondType) {
		return firstType.equals(secondType);
	}

	/**
	 * Generates from a gateway rpc method signature the corresponding rpc endpoint signature.
	 *
	 * For example the {@link RpcTimeout} annotation adds an additional parameter to the gateway
	 * signature which is not relevant on the server side.
	 *
	 * @param method Method to generate the signature string for
	 * @return String of the respective server side rpc method signature
	 */
	private String generateEndpointMethodSignature(Method method) {
		StringBuilder builder = new StringBuilder();

		if (method.getReturnType().equals(Void.TYPE)) {
			builder.append("void").append(" ");
		} else if (method.getReturnType().equals(futureClass)) {
			Class<?> valueClass = ReflectionUtil.getTemplateType1(method.getGenericReturnType());

			builder
				.append(futureClass.getSimpleName())
				.append("<")
				.append(valueClass != null ? valueClass.getSimpleName() : "")
				.append(">");

			if (valueClass != null) {
				builder.append("/").append(valueClass.getSimpleName());
			}

			builder.append(" ");
		} else {
			return "Invalid rpc method signature.";
		}

		builder.append(method.getName()).append("(");

		Class<?>[] parameterTypes = method.getParameterTypes();
		Annotation[][] parameterAnnotations = method.getParameterAnnotations();

		assertEquals(parameterTypes.length, parameterAnnotations.length);

		for (int i = 0; i < parameterTypes.length; i++) {
			// filter out the RpcTimeout parameters
			if (!isRpcTimeout(parameterAnnotations[i])) {
				builder.append(parameterTypes[i].getName());

				if (i < parameterTypes.length -1) {
					builder.append(", ");
				}
			}
		}

		builder.append(")");

		return builder.toString();
	}

	private boolean isRpcTimeout(Annotation[] annotations) {
		for (Annotation annotation : annotations) {
			if (annotation.annotationType().equals(RpcTimeout.class)) {
				return true;
			}
		}

		return false;
	}
}
