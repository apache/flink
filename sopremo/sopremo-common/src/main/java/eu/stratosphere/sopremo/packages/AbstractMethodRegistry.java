/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.packages;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.function.JavaMethod;
import eu.stratosphere.sopremo.function.SopremoMethod;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * @author Arvid Heise
 */
public abstract class AbstractMethodRegistry implements IMethodRegistry {

	private static boolean isCompatibleSignature(final Method method) {
		if (!IJsonNode.class.isAssignableFrom(method.getReturnType()))
			return false;

		boolean compatibleSignature;
		final Class<?>[] parameterTypes = method.getParameterTypes();
		if (parameterTypes.length == 1 && parameterTypes[0].isArray()
			&& IJsonNode.class.isAssignableFrom(parameterTypes[0].getComponentType()))
			compatibleSignature = true;
		else {
			compatibleSignature = true;
			for (int index = 0; index < parameterTypes.length; index++)
				if (!IJsonNode.class.isAssignableFrom(parameterTypes[index])
					&& !(index == parameterTypes.length - 1 && method.isVarArgs() &&
					IJsonNode.class.isAssignableFrom(parameterTypes[index].getComponentType()))) {
					compatibleSignature = false;
					break;
				}
		}
		return compatibleSignature;
	}

	@Override
	public abstract SopremoMethod findMethod(String name);

	@Override
	public abstract void registerMethod(String name, SopremoMethod method);

	public void register(final Class<?> javaFunctions) {
		final List<Method> functions = getCompatibleMethods(
			ReflectUtil.getMethods(javaFunctions, null, Modifier.STATIC | Modifier.PUBLIC));

		for (final Method method : functions)
			this.register(method);

		if (FunctionRegistryCallback.class.isAssignableFrom(javaFunctions))
			((FunctionRegistryCallback) ReflectUtil.newInstance(javaFunctions)).registerFunctions(this);
	}

	@Override
	public void register(final Method method) {
		SopremoMethod javaMethod = findMethod(method.getName());
		if (javaMethod == null || !(javaMethod instanceof JavaMethod))
			this.registerMethod(method.getName(), javaMethod = new JavaMethod(method.getName()));
		((JavaMethod) javaMethod).addSignature(method);
	}

	public static List<Method> getCompatibleMethods(final List<Method> methods) {
		final List<Method> functions = new ArrayList<Method>();
		for (final Method method : methods)
			if (isCompatibleSignature(method))
				functions.add(method);
		return functions;
	}
}
