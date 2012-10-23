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

package eu.stratosphere.pact.contextcheck;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.pact.common.plan.Visitable;
import eu.stratosphere.pact.common.plan.Visitor;

public abstract class ReflectionVisitor<T extends Visitable<T>> implements Visitor<T> {
	private final Map<Class<?>, Method> preVisits = new HashMap<Class<?>, Method>();

	private final Map<Class<?>, Method> postVisits = new HashMap<Class<?>, Method>();

	protected ReflectionVisitor() {
		{
			Method[] preVisitsMethods = getClass().getDeclaredMethods();
			for (Method method : preVisitsMethods) {
				if (method.getName().startsWith("preVisit"))
					preVisits.put(method.getParameterTypes()[0], method);
			}
		}
		{
			Method[] prostVisitsMethods = getClass().getDeclaredMethods();
			for (Method method : prostVisitsMethods) {
				if (method.getName().startsWith("postVisit"))
					postVisits.put(method.getParameterTypes()[0], method);
			}
		}
	}

	public boolean preVisit(T node) {
		try {
			Class<?> clazz = node.getClass();
			while (Arrays.asList(clazz.getInterfaces()).contains(Visitable.class)) {
				if (preVisits.containsKey(clazz)) {
					preVisits.get(clazz).invoke(this, node);
					return true;
				} else {
					clazz = clazz.getSuperclass();
				}
			}
			preVisitDefault(node);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return true;
	}

	public void postVisit(T node) {
		try {
			Class<?> clazz = node.getClass();
			while (Arrays.asList(clazz.getInterfaces()).contains(Visitable.class)) {
				if (postVisits.containsKey(clazz)) {
					postVisits.get(clazz).invoke(this, node);
					return;
				} else {
					clazz = clazz.getSuperclass();
				}
			}
			postVisitDefault(node);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	void preVisitDefault(T node) {
	}

	void postVisitDefault(T node) {
	}
}
