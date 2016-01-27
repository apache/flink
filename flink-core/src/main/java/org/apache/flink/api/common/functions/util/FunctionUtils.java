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

package org.apache.flink.api.common.functions.util;

import java.lang.reflect.Method;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

/**
 * Utility class that contains helper methods to work with Flink {@link Function} class.
 */
public final class FunctionUtils {

	public static void openFunction(Function function, Configuration parameters) throws Exception{
		if (function instanceof RichFunction) {
			RichFunction richFunction = (RichFunction) function;
			richFunction.open(parameters);
		}
	}

	public static void closeFunction(Function function) throws Exception{
		if (function instanceof RichFunction) {
			RichFunction richFunction = (RichFunction) function;
			richFunction.close();
		}
	}

	public static void setFunctionRuntimeContext(Function function, RuntimeContext context){
		if (function instanceof RichFunction) {
			RichFunction richFunction = (RichFunction) function;
			richFunction.setRuntimeContext(context);
		}
	}

	public static RuntimeContext getFunctionRuntimeContext(Function function, RuntimeContext defaultContext){
		if (function instanceof RichFunction) {
			RichFunction richFunction = (RichFunction) function;
			return richFunction.getRuntimeContext();
		}
		else {
			return defaultContext;
		}
	}
	
	public static Method checkAndExtractLambdaMethod(Function function) {
		try {
			// get serialized lambda
			Object serializedLambda = null;
			for (Class<?> clazz = function.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
				try {
					Method replaceMethod = clazz.getDeclaredMethod("writeReplace");
					replaceMethod.setAccessible(true);
					Object serialVersion = replaceMethod.invoke(function);

					// check if class is a lambda function
					if (serialVersion.getClass().getName().equals("java.lang.invoke.SerializedLambda")) {

						// check if SerializedLambda class is present
						try {
							Class.forName("java.lang.invoke.SerializedLambda");
						}
						catch (Exception e) {
							throw new UnsupportedOperationException("User code tries to use lambdas, but framework is running with a Java version < 8");
						}
						serializedLambda = serialVersion;
						break;
					}
				}
				catch (NoSuchMethodException e) {
					// thrown if the method is not there. fall through the loop
				}
			}

			// not a lambda method -> return null
			if (serializedLambda == null) {
				return null;
			}

			// find lambda method
			Method implClassMethod = serializedLambda.getClass().getDeclaredMethod("getImplClass");
			Method implMethodNameMethod = serializedLambda.getClass().getDeclaredMethod("getImplMethodName");

			String className = (String) implClassMethod.invoke(serializedLambda);
			String methodName = (String) implMethodNameMethod.invoke(serializedLambda);

			Class<?> implClass = Class.forName(className.replace('/', '.'), true, Thread.currentThread().getContextClassLoader());

			Method[] methods = implClass.getDeclaredMethods();
			Method parameterizedMethod = null;
			for (Method method : methods) {
				if(method.getName().equals(methodName)) {
					if(parameterizedMethod != null) {
						// It is very unlikely that a class contains multiple e.g. "lambda$2()" but its possible
						// Actually, the signature need to be checked, but this is very complex
						throw new Exception("Lambda method name is not unique.");
					}
					else {
						parameterizedMethod = method;
					}
				}
			}
			if (parameterizedMethod == null) {
				throw new Exception("No lambda method found.");
			}
			return parameterizedMethod;
		}
		catch (Exception e) {
			throw new RuntimeException("Could not extract lambda method out of function: " + e.getClass().getSimpleName() + " - " + e.getMessage(), e);
		}
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private FunctionUtils() {
		throw new RuntimeException();
	}
}
