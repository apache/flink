/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.sopremo.type;

import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import eu.stratosphere.pact.common.util.ReflectionUtil;

/**
 * @author Arvid Heise
 */
public class TemporaryVariableFactory {
	public final static TemporaryVariableFactory INSTANCE = new TemporaryVariableFactory();
	
	private ThreadLocal<Map<Class<?>, Queue<Object>>> cachedObjects = new ThreadLocal<Map<Class<?>, Queue<Object>>>() {
		/*
		 * (non-Javadoc)
		 * @see java.lang.ThreadLocal#initialValue()
		 */
		@Override
		protected Map<Class<?>, Queue<Object>> initialValue() {
			return new IdentityHashMap<Class<?>, Queue<Object>>();
		}
	};

	@SuppressWarnings("unchecked")
	public <T> T alllocateVariable(Class<T> type) {
		Queue<Object> objectList = getValueList(type);
		final T oldObject = (T) objectList.poll();
		return oldObject == null ? ReflectionUtil.newInstance(type) : oldObject;
	}
	
	private final Queue<Object> getValueList(Class<?> type) {
		final Map<Class<?>, Queue<Object>> classToInstances = this.cachedObjects.get();
		Queue<Object> objectList = classToInstances .get(type);
		if(objectList == null)
			classToInstances.put(type, objectList = new LinkedList<Object>());
		return objectList;
	}
	
	public void free(Object object) {
		getValueList(object.getClass()).offer(object);
	}
}
