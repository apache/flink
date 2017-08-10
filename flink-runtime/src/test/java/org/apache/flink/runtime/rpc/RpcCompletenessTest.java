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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.ReflectionUtil;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test which ensures that all classes of subtype {@link RpcEndpoint} implement
 * the methods specified in the generic gateway type argument.
 *
 * {@code
 * 	    RpcEndpoint<GatewayTypeParameter extends RpcGateway>
 * }
 *
 * Note, that the class hierarchy can also be nested. In this case the type argument
 * always has to be the first argument, e.g. {@code
 *
 * 	    // RpcClass needs to implement RpcGatewayClass' methods
 * 	    RpcClass extends RpcEndpoint<RpcGatewayClass>
 *
 * 	    // RpcClass2 or its subclass needs to implement RpcGatewayClass' methods
 *      RpcClass<GatewayTypeParameter extends RpcGateway,...> extends RpcEndpoint<GatewayTypeParameter>
 *      RpcClass2 extends RpcClass<RpcGatewayClass,...>
 *
 *      // needless to say, this can even be nested further
 *      ...
 * }
 *
 */
public class RpcCompletenessTest extends TestLogger {

	private static Logger LOG = LoggerFactory.getLogger(RpcCompletenessTest.class);

	private static final Class<?> futureClass = CompletableFuture.class;
	private static final Class<?> timeoutClass = Time.class;

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void testRpcCompleteness() {
		Reflections reflections = new Reflections("org.apache.flink");

		Set<Class<? extends RpcEndpoint>> classes = reflections.getSubTypesOf(RpcEndpoint.class);

		Class<? extends RpcEndpoint> c;

		mainloop:
		for (Class<? extends RpcEndpoint> rpcEndpoint : classes) {
			c = rpcEndpoint;

			LOG.debug("-------------");
			LOG.debug("c: {}", c);

			// skip abstract classes
			if (Modifier.isAbstract(c.getModifiers())) {
				LOG.debug("Skipping abstract class");
				continue;
			}

			// check for type parameter bound to RpcGateway
			// skip if one is found because a subclass will provide the concrete argument
			TypeVariable<? extends Class<? extends RpcEndpoint>>[] typeParameters = c.getTypeParameters();
			LOG.debug("Checking {} parameters.", typeParameters.length);
			for (int i = 0; i < typeParameters.length; i++) {
				for (Type bound : typeParameters[i].getBounds()) {
					LOG.debug("checking bound {} of type parameter {}", bound, typeParameters[i]);
					if (bound.toString().equals("interface " + RpcGateway.class.getName())) {
						if (i > 0) {
							fail("Type parameter for RpcGateway should come first in " + c);
						}
						LOG.debug("Skipping class with type parameter bound to RpcGateway.");
						// Type parameter is bound to RpcGateway which a subclass will provide
						continue mainloop;
					}
				}
			}

			// check if this class or any super class contains the RpcGateway argument
			Class<?> rpcGatewayType;
			do {
				LOG.debug("checking type argument of class: {}", c);
				rpcGatewayType = ReflectionUtil.getTemplateType1(c);
				LOG.debug("type argument is: {}", rpcGatewayType);

				c = (Class<? extends RpcEndpoint>) c.getSuperclass();

			} while (!RpcGateway.class.isAssignableFrom(rpcGatewayType));

			LOG.debug("Checking RRC completeness of endpoint '{}' with gateway '{}'",
				rpcEndpoint.getSimpleName(), rpcGatewayType.getSimpleName());

			checkCompleteness(rpcEndpoint, (Class<? extends RpcGateway>) rpcGatewayType);
		}
	}

	@SuppressWarnings("rawtypes")
	private void checkCompleteness(Class<? extends RpcEndpoint> rpcEndpoint, Class<? extends RpcGateway> rpcGateway) {
		List<Method> rpcMethodsFromGateway = getRpcMethodsFromGateway(rpcGateway);
		Method[] gatewayMethods = rpcMethodsFromGateway.toArray(new Method[rpcMethodsFromGateway.size()]);
		Method[] serverMethods = rpcEndpoint.getMethods();

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
	 *     <li>It checks whether the return type is void or a {@link CompletableFuture} wrapping the actual result. </li>
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
			if (RpcCompletenessTest.isRpcTimeout(parameterAnnotations[i])) {
				assertTrue(
					"The rpc timeout has to be of type " + timeoutClass.getName() + ".",
					parameterTypes[i].equals(timeoutClass));

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
			if (!RpcCompletenessTest.isRpcTimeout(gatewayParameterAnnotations[i])) {
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
					ReflectionUtil.FullTypeInfo fullValueTypeInfo = ReflectionUtil.getFullTemplateType(gatewayMethod.getGenericReturnType(), 0);

					if (endpointMethod.getReturnType().equals(futureClass)) {
						ReflectionUtil.FullTypeInfo fullRpcEndpointValueTypeInfo = ReflectionUtil.getFullTemplateType(endpointMethod.getGenericReturnType(), 0);

						// check if we have the same future value types
						if (fullValueTypeInfo != null && fullRpcEndpointValueTypeInfo != null) {
							Iterator<Class<?>> valueClasses = fullValueTypeInfo.getClazzIterator();
							Iterator<Class<?>> rpcClasses = fullRpcEndpointValueTypeInfo.getClazzIterator();

							while (valueClasses.hasNext() && rpcClasses.hasNext()) {
								if (!checkType(valueClasses.next(), rpcClasses.next())) {
									return false;
								}
							}

							// both should be empty
							return !valueClasses.hasNext() && !rpcClasses.hasNext();
						}
					} else {
						if (fullValueTypeInfo != null && !checkType(fullValueTypeInfo.getClazz(), endpointMethod.getReturnType())) {
							return false;
						}
					}
				}
			}

			return gatewayMethod.getName().equals(endpointMethod.getName());
		}
	}

	private boolean checkType(Class<?> firstType, Class<?> secondType) {
		Class<?> firstResolvedType;
		Class<?> secondResolvedType;

		if (firstType.isPrimitive()) {
			firstResolvedType = RpcCompletenessTest.resolvePrimitiveType(firstType);
		} else {
			firstResolvedType = firstType;
		}

		if (secondType.isPrimitive()) {
			secondResolvedType = RpcCompletenessTest.resolvePrimitiveType(secondType);
		} else {
			secondResolvedType = secondType;
		}

		return firstResolvedType.equals(secondResolvedType);
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
			ReflectionUtil.FullTypeInfo fullTypeInfo = ReflectionUtil.getFullTemplateType(method.getGenericReturnType(), 0);

			builder
				.append(futureClass.getSimpleName())
				.append("<")
				.append(fullTypeInfo != null ? fullTypeInfo.toString() : "")
				.append(">");

			if (fullTypeInfo != null) {
				builder.append("/").append(fullTypeInfo);
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
			if (!RpcCompletenessTest.isRpcTimeout(parameterAnnotations[i])) {
				builder.append(parameterTypes[i].getName());

				if (i < parameterTypes.length -1) {
					builder.append(", ");
				}
			}
		}

		builder.append(")");

		return builder.toString();
	}

	private static boolean isRpcTimeout(Annotation[] annotations) {
		for (Annotation annotation : annotations) {
			if (annotation.annotationType().equals(RpcTimeout.class)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Returns the boxed type for a primitive type.
	 *
	 * @param primitveType Primitive type to resolve
	 * @return Boxed type for the given primitive type
	 */
	private static Class<?> resolvePrimitiveType(Class<?> primitveType) {
		assert primitveType.isPrimitive();

		TypeInformation<?> typeInformation = BasicTypeInfo.getInfoFor(primitveType);

		if (typeInformation != null) {
			return typeInformation.getTypeClass();
		} else {
			throw new RuntimeException("Could not retrive basic type information for primitive type " + primitveType + '.');
		}
	}

	/**
	 * Extract all rpc methods defined by the gateway interface
	 *
	 * @param interfaceClass the given rpc gateway interface
	 * @return all methods defined by the given interface
	 */
	private List<Method> getRpcMethodsFromGateway(Class<? extends RpcGateway> interfaceClass) {
		if(!interfaceClass.isInterface()) {
			fail(interfaceClass.getName() + " is not a interface");
		}

		ArrayList<Method> allMethods = new ArrayList<>();
		// Methods defined in RpcGateway are native method
		if(interfaceClass.equals(RpcGateway.class)) {
			return allMethods;
		}

		// Get all methods declared in current interface
		Collections.addAll(allMethods, interfaceClass.getDeclaredMethods());

		// Get all method inherited from super interface
		for (Class<?> superClass : interfaceClass.getInterfaces()) {
			@SuppressWarnings("unchecked")
			Class<? extends RpcGateway> gatewayClass = (Class<? extends RpcGateway>) superClass;
			allMethods.addAll(getRpcMethodsFromGateway(gatewayClass));
		}
		return allMethods;
	}
}
