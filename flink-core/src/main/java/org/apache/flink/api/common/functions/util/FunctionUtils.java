/**
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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.lang.reflect.Method;

public class FunctionUtils {

	public static void openFunction (Function function, Configuration parameters) throws Exception{
		if (function instanceof RichFunction) {
			RichFunction richFunction = (RichFunction) function;
			richFunction.open (parameters);
		}
	}

	public static void closeFunction (Function function) throws Exception{
		if (function instanceof RichFunction) {
			RichFunction richFunction = (RichFunction) function;
			richFunction.close ();
		}
	}

	public static void setFunctionRuntimeContext (Function function, RuntimeContext context){
		if (function instanceof RichFunction) {
			RichFunction richFunction = (RichFunction) function;
			richFunction.setRuntimeContext(context);
		}
	}

	public static RuntimeContext getFunctionRuntimeContext (Function function, RuntimeContext defaultContext){
		if (function instanceof RichFunction) {
			RichFunction richFunction = (RichFunction) function;
			return richFunction.getRuntimeContext();
		}
		else {
			return defaultContext;
		}
	}

	public static boolean isLambdaFunction(Function function) {
		if (function == null) {
			throw new IllegalArgumentException();
		}
		
		for (Class<?> clazz = function.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
			try {
				Method replaceMethod = clazz.getDeclaredMethod("writeReplace");
				replaceMethod.setAccessible(true);
				Object serialVersion = replaceMethod.invoke(function);
				
				if (serialVersion.getClass().getName().equals("java.lang.invoke.SerializedLambda")) {
					return true;
				}
			}
			catch (NoSuchMethodException e) {
				// thrown if the method is not there. fall through the loop
			}
			catch (Throwable t) {
				// this should not happen, we are not executing any method code.
				throw new RuntimeException("Error while checking whether function is a lambda.", t);
			}
		}
		
		return false;
	}
}
