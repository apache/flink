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

package org.apache.flink.api.common.operators.util;

import java.lang.annotation.Annotation;

import org.apache.flink.util.InstantiationUtil;

/**
 * This holds a class containing user defined code.
 */
public class UserCodeClassWrapper<T> implements UserCodeWrapper<T> {
	private static final long serialVersionUID = 1L;
	
	private Class<? extends T> userCodeClass;
	
	public UserCodeClassWrapper(Class<? extends T> userCodeClass) {
		this.userCodeClass = userCodeClass;
	}
	
	@Override
	public T getUserCodeObject(Class<? super T> superClass, ClassLoader cl) {
		return InstantiationUtil.instantiate(userCodeClass, superClass);
	}
	
	@Override
	public T getUserCodeObject() {
		return InstantiationUtil.instantiate(userCodeClass, Object.class);
	}

	@Override
	public <A extends Annotation> A getUserCodeAnnotation(Class<A> annotationClass) {
		return userCodeClass.getAnnotation(annotationClass);
	}
	
	@Override
	public Class<? extends T> getUserCodeClass() {
		return userCodeClass;
	}
}
