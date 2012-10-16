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
package eu.stratosphere.util.reflect;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author Arvid Heise
 */
public class ExtensionMethod<ReturnType> extends DynamicMethod<ReturnType> {

	/**
	 * Initializes ExtensionMethod.
	 * 
	 * @param name
	 */
	public ExtensionMethod(final String name) {
		super(name);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -6283761940969938808L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.reflect.DynamicInvokable#getSignatureTypes(java.lang.reflect.Member)
	 */
	@Override
	protected Class<?>[] getSignatureTypes(final Method method) {
		Class<?>[] signatureTypes = super.getSignatureTypes(method);
		if (this.needsInstance(method)) {
			final ObjectArrayList<Class<?>> paramList = ObjectArrayList.wrap(signatureTypes);
			paramList.ensureCapacity(paramList.size() + 1);
			paramList.add(0, method.getDeclaringClass());
			signatureTypes = paramList.elements();
		}
		return signatureTypes;
	}

	public ReturnType invoke(final Object... params) throws Exception {
		return super.invoke(null, params);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.reflect.DynamicMethod#invokeDirectly(java.lang.reflect.Method, java.lang.Object,
	 * java.lang.Object[])
	 */
	@Override
	protected ReturnType invokeDirectly(final Method method, final Object context, final Object[] params)
			throws IllegalAccessException,
			InvocationTargetException {
		if (this.needsInstance(method) && context == null)
			return super.invokeDirectly(method, params[0], ObjectArrayList.wrap(params).subList(1, params.length)
				.toArray());
		return super.invokeDirectly(method, context, params);
	}
}
