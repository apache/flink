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

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RpcCompletenessTest extends TestLogger {

	@Test
	public void testRpcCompleteness() {
		Reflections reflections = new Reflections("org.apache.flink");

		Set<Class<? extends RpcServer>> classes = reflections.getSubTypesOf(RpcServer.class);

		Class<? extends RpcServer> c = null;

		for (Class<? extends RpcServer> rpcServer :classes){
			c = rpcServer;
			Type superClass = c.getGenericSuperclass();

			boolean foundRpcServerInterface = false;

			if (superClass instanceof ParameterizedType) {
				ParameterizedType parameterizedType = (ParameterizedType) superClass;

				if (parameterizedType.getRawType() == RpcServer.class) {
					foundRpcServerInterface = true;
					Type[] typeArguments = parameterizedType.getActualTypeArguments();

					assertEquals(1, typeArguments.length);
					assertTrue(typeArguments[0] instanceof Class<?>);

					Type rpcGatewayType = typeArguments[0];

					assertTrue(rpcGatewayType instanceof Class);

					checkCompleteness(rpcServer, (Class<?>) rpcGatewayType);
				}
			}

			assertTrue("The class " + rpcServer + " does not implement the " + RpcServer.class + " interface.", foundRpcServerInterface);
		}
	}

	private void checkCompleteness(Class<?> rpcServer, Class<?> rpcGateway) {
		Method[] gatewayMethods = rpcGateway.getDeclaredMethods();
		Method[] serverMethods = rpcServer.getDeclaredMethods();

		Map<String, List<Method>> rpcMethods = new HashMap<>();
		int numberServerRpcMethods = 0;

		for (Method serverMethod : serverMethods) {
			if (serverMethod.isAnnotationPresent(RpcMethod.class)) {
				if (rpcMethods.containsKey(serverMethod.getName())) {
					List<Method> methods = rpcMethods.get(serverMethod.getName());
					methods.add(serverMethod);

					rpcMethods.put(serverMethod.getName(), methods);
				} else {
					List<Method> methods = new ArrayList<>();
					methods.add(serverMethod);

					rpcMethods.put(serverMethod.getName(), methods);
				}

				numberServerRpcMethods++;
			}
		}

		assertEquals(
			"Server class " + rpcServer + " does not have the same number of rpc methods than " +
				"the gateway class " + rpcGateway ,
			gatewayMethods.length,
			numberServerRpcMethods);

		for (Method gatewayMethod : gatewayMethods) {
			assertTrue(rpcMethods.containsKey(gatewayMethod.getName()));

			checkGatewayMethod(gatewayMethod, rpcMethods.get(gatewayMethod.getName()));
		}
	}

	/**
	 * Checks whether we find a matching overloaded version for the gateway method among the methods
	 * with the same name in the rpc server.
	 *
	 * @param gatewayMethod Gateway method
	 * @param rpcMethods List of rpc methods on the rpc server with the same name as the gateway
	 *                   method
	 */
	private void checkGatewayMethod(Method gatewayMethod, List<Method> rpcMethods) {
		for (Method rpcMethod : rpcMethods) {
			if (checkMethod(gatewayMethod, rpcMethod)) {
				return;
			}
		}

		fail("Could not find rpc method which is compatible to " + gatewayMethod);
	}

	private boolean checkMethod(Method gatewayMethod, Method rpcMethod) {
		Class<?>[] firstParameterTypes = gatewayMethod.getParameterTypes();
		Class<?>[] secondParameterTypes = rpcMethod.getParameterTypes();

		if (firstParameterTypes.length != secondParameterTypes.length) {
			return false;
		} else {
			// check the parameter types
			for (int i = 0; i < firstParameterTypes.length; i++) {
				if (!checkType(firstParameterTypes[i], secondParameterTypes[i])) {
					return false;
				}
			}

			// check the return types
			if (rpcMethod.getReturnType() == void.class) {
				if (gatewayMethod.getReturnType() != void.class) {
					return false;
				}
			} else {
				// has return value. The gateway method should be wrapped in a future
				Class<?> futureClass = gatewayMethod.getReturnType();

				if (futureClass != Future.class) {
					return false;
				}

				Type futureType = gatewayMethod.getGenericReturnType();

				if (futureType instanceof ParameterizedType) {
					ParameterizedType parameterizedType = (ParameterizedType) futureType;

					Type[] typeArguments = parameterizedType.getActualTypeArguments();

					// check that we only have one type argument
					if (typeArguments.length == 1) {
						Type typeArgument = typeArguments[0];

						// check that the type argument is a Class
						if (typeArgument instanceof Class<?>) {
							if (!checkType((Class<?>) typeArgument, rpcMethod.getReturnType())) {
								return false;
							}
						}
					} else {
						return false;
					}
				}


			}

			return gatewayMethod.getName().equals(rpcMethod.getName());
		}
	}

	private boolean checkType(Class<?> firstType, Class<?> secondType) {
		return firstType == secondType;
	}
}
