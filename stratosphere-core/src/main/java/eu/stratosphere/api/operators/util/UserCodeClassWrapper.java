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
package eu.stratosphere.api.operators.util;

import java.lang.annotation.Annotation;

import eu.stratosphere.util.InstantiationUtil;

/**
 * This holds a class containing user defined code.
 */
public class UserCodeClassWrapper<T> implements UserCodeWrapper<T> {
	private static final long serialVersionUID = 1L;
	
	private Class<? extends T> userCodeClass;
	private String className;
	
	public UserCodeClassWrapper(Class<? extends T> userCodeClass) {
		this.userCodeClass = userCodeClass;
		this.className = userCodeClass.getName();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public T getUserCodeObject(Class<? super T> superClass, ClassLoader cl) {
		try {
			Class<T> clazz = (Class<T>) Class.forName(className, true, cl).asSubclass(superClass);
			return InstantiationUtil.instantiate(clazz, superClass);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("User code class could not be instantiated: " + e);
		}
	}
	
	@Override
	public T getUserCodeObject() {
		try {
			return userCodeClass.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException("User code class could not be instantiated: " + e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("User code class could not be instantiated: " + e);
		}
	}

	@Override
	public <A extends Annotation> A getUserCodeAnnotation(
			Class<A> annotationClass) {
		return userCodeClass.getAnnotation(annotationClass);
	}
	
	@Override
	public Class<? extends T> getUserCodeClass() {
		return userCodeClass;
	}
}
