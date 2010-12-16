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

package eu.stratosphere.pact.common.util;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @TODO
 * @author Erik Nijkamp
 */
public class ReflectionUtil {
	public static <T> T newInstance(Class<T> clazz) {
		try {
			return clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> getTemplateType1(Class<?> clazz) {
		return (Class<T>) getSuperTemplateTypes(clazz)[0];
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> getTemplateType2(Class<?> clazz) {
		return (Class<T>) getSuperTemplateTypes(clazz)[1];
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> getTemplateType3(Class<?> clazz) {
		return (Class<T>) getSuperTemplateTypes(clazz)[2];
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> getTemplateType4(Class<?> clazz) {
		return (Class<T>) getSuperTemplateTypes(clazz)[3];
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> getTemplateType5(Class<?> clazz) {
		return (Class<T>) getSuperTemplateTypes(clazz)[4];
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> getTemplateType6(Class<?> clazz) {
		return (Class<T>) getSuperTemplateTypes(clazz)[5];
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> getTemplateType7(Class<?> clazz) {
		return (Class<T>) getSuperTemplateTypes(clazz)[6];
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> getTemplateType8(Class<?> clazz) {
		return (Class<T>) getSuperTemplateTypes(clazz)[7];
	}

	public static Class<?>[] getSuperTemplateTypes(Class<?> clazz) {
		Type type = clazz.getGenericSuperclass();
		while (true) {
			if (type instanceof ParameterizedType) {
				return getTemplateTypes((ParameterizedType) type);
			}

			if (clazz.getGenericSuperclass() == null) {
				throw new IllegalArgumentException();
			}

			type = clazz.getGenericSuperclass();
			clazz = clazz.getSuperclass();
		}
	}

	public static Class<?>[] getTemplateTypes(ParameterizedType type) {
		assert (type instanceof ParameterizedType);
		ParameterizedType paramterizedType = (ParameterizedType) type;
		Class<?>[] types = new Class<?>[paramterizedType.getActualTypeArguments().length];
		int i = 0;
		for (Type templateArgument : paramterizedType.getActualTypeArguments()) {
			assert (templateArgument instanceof Class<?>);
			types[i++] = (Class<?>) templateArgument;
		}
		return types;
	}

	public static Class<?>[] getTemplateTypes(Class<?> clazz) {
		Type type = clazz.getGenericSuperclass();
		assert (type instanceof ParameterizedType);
		ParameterizedType paramterizedType = (ParameterizedType) type;
		Class<?>[] types = new Class<?>[paramterizedType.getActualTypeArguments().length];
		int i = 0;
		for (Type templateArgument : paramterizedType.getActualTypeArguments()) {
			assert (templateArgument instanceof Class<?>);
			types[i++] = (Class<?>) templateArgument;
		}
		return types;
	}
}
