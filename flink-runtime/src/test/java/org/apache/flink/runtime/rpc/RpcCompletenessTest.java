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

import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.reflections.Reflections;
import scala.concurrent.Future;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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

		Set<Class<? extends RpcProtocol>> classes = reflections.getSubTypesOf(RpcProtocol.class);

		Class<? extends RpcProtocol> c;

		for (Class<? extends RpcProtocol> rpcProtocol :classes){
			c = rpcProtocol;
			Type superClass = c.getGenericSuperclass();

			Class<?> rpcGatewayType = extractTypeParameter(superClass, 0);

			if (rpcGatewayType != null) {
				checkCompleteness(rpcProtocol, (Class<? extends RpcGateway>) rpcGatewayType);
			} else {
				fail("Could not retrieve the rpc gateway class for the given rpc protocol class " + rpcProtocol.getName());
			}
		}
	}

	private void checkCompleteness(Class<? extends RpcProtocol> rpcProtocol, Class<? extends RpcGateway> rpcGateway) {
		Method[] gatewayMethods = rpcGateway.getDeclaredMethods();
		Method[] serverMethods = rpcProtocol.getDeclaredMethods();

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
				"The rpc protocol " + rpcProtocol.getName() + " does not contain a RpcMethod " +
					"annotated method with the same name and signature " +
					generateProtocolMethodSignature(gatewayMethod) + ".",
				rpcMethods.containsKey(gatewayMethod.getName()));

			checkGatewayMethod(gatewayMethod);

			if (!matchGatewayMethodWithProtocol(gatewayMethod, rpcMethods.get(gatewayMethod.getName()), unmatchedRpcMethods)) {
				fail("Could not find a RpcMethod annotated method in rpc protocol " +
					rpcProtocol.getName() + " matching the rpc gateway method " +
					generateProtocolMethodSignature(gatewayMethod) + " defined in the rpc gateway " +
					rpcGateway.getName() + ".");
			}
		}

		if (!unmatchedRpcMethods.isEmpty()) {
			StringBuilder builder = new StringBuilder();

			for (Method unmatchedRpcMethod : unmatchedRpcMethods) {
				builder.append(unmatchedRpcMethod).append("\n");
			}

			fail("The rpc protocol " + rpcProtocol.getName() + " contains rpc methods which " +
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
		int rpcTimeoutParameters = 0;

		for (Annotation[] parameterAnnotation : parameterAnnotations) {
			for (Annotation annotation : parameterAnnotation) {
				if (annotation.equals(RpcTimeout.class)) {
					rpcTimeoutParameters++;
				}
			}
		}

		assertTrue("The gateway method " + gatewayMethod + " must have at most one RpcTimeout " +
			"annotated parameter.", rpcTimeoutParameters <= 1);
	}

	/**
	 * Checks whether we find a matching overloaded version for the gateway method among the methods
	 * with the same name in the rpc protocol.
	 *
	 * @param gatewayMethod Gateway method
	 * @param protocolMethods Set of rpc methods on the rpc server with the same name as the gateway
	 *                   method
	 * @param unmatchedRpcMethods Set of unmatched rpc methods on the protocol side (so far)
	 */
	private boolean matchGatewayMethodWithProtocol(Method gatewayMethod, Set<Method> protocolMethods, Set<Method> unmatchedRpcMethods) {
		for (Method protocolMethod : protocolMethods) {
			if (checkMethod(gatewayMethod, protocolMethod)) {
				unmatchedRpcMethods.remove(protocolMethod);
				return true;
			}
		}

		return false;
	}

	private boolean checkMethod(Method gatewayMethod, Method protocolMethod) {
		Class<?>[] gatewayParameterTypes = gatewayMethod.getParameterTypes();
		Annotation[][] gatewayParameterAnnotations = gatewayMethod.getParameterAnnotations();

		Class<?>[] protocolParameterTypes = protocolMethod.getParameterTypes();

		List<Class<?>> filteredGatewayParameterTypes = new ArrayList<>();

		assertEquals(gatewayParameterTypes.length, gatewayParameterAnnotations.length);

		// filter out the RpcTimeout parameters
		for (int i = 0; i < gatewayParameterTypes.length; i++) {
			if (!isRpcTimeout(gatewayParameterAnnotations[i])) {
				filteredGatewayParameterTypes.add(gatewayParameterTypes[i]);
			}
		}

		if (filteredGatewayParameterTypes.size() != protocolParameterTypes.length) {
			return false;
		} else {
			// check the parameter types
			for (int i = 0; i < filteredGatewayParameterTypes.size(); i++) {
				if (!checkType(filteredGatewayParameterTypes.get(i), protocolParameterTypes[i])) {
					return false;
				}
			}

			// check the return types
			if (protocolMethod.getReturnType() == void.class) {
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
					Class<?> valueClass = extractTypeParameter(futureClass, 0);

					if (protocolMethod.getReturnType().equals(futureClass)) {
						Class<?> rpcProtocolValueClass = extractTypeParameter(protocolMethod.getReturnType(), 0);

						// check if we have the same future value types
						if (valueClass != null && rpcProtocolValueClass != null && !checkType(valueClass, rpcProtocolValueClass)) {
							return false;
						}
					} else {
						if (valueClass != null && !checkType(valueClass, protocolMethod.getReturnType())) {
							return false;
						}
					}
				}
			}

			return gatewayMethod.getName().equals(protocolMethod.getName());
		}
	}

	private boolean checkType(Class<?> firstType, Class<?> secondType) {
		return firstType.equals(secondType);
	}

	/**
	 * Generates from a gateway rpc method signature the corresponding rpc protocol signature.
	 *
	 * For example the {@link RpcTimeout} annotation adds an additional parameter to the gateway
	 * signature which is not relevant on the server side.
	 *
	 * @param method Method to generate the signature string for
	 * @return String of the respective server side rpc method signature
	 */
	private String generateProtocolMethodSignature(Method method) {
		StringBuilder builder = new StringBuilder();

		if (method.getReturnType().equals(Void.TYPE)) {
			builder.append("void").append(" ");
		} else if (method.getReturnType().equals(futureClass)) {
			Class<?> valueClass = extractTypeParameter(method.getGenericReturnType(), 0);

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

	private Class<?> extractTypeParameter(Type genericType, int position) {
		if (genericType instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) genericType;

			Type[] typeArguments = parameterizedType.getActualTypeArguments();

			if (position < 0 || position >= typeArguments.length) {
				throw new IndexOutOfBoundsException("The generic type " +
					parameterizedType.getRawType() + " only has " + typeArguments.length +
					" type arguments.");
			} else {
				Type typeArgument = typeArguments[position];

				if (typeArgument instanceof Class<?>) {
					return (Class<?>) typeArgument;
				} else {
					return null;
				}
			}
		} else {
			return null;
		}
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
