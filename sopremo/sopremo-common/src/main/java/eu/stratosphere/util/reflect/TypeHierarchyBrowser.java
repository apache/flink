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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.util.ConcatenatingIterable;

/**
 * A facility to easily browse through the hierarchy of types.
 * 
 * @author Arvid Heise
 */
public class TypeHierarchyBrowser {
	public static TypeHierarchyBrowser INSTANCE = new TypeHierarchyBrowser();

	public enum Mode {
		CLASS_ONLY {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.util.reflect.ClassHierarchyVisiter.Mode#getSuperTypes(java.lang.Class)
			 */
			@Override
			Iterable<? extends Class<?>> getSuperTypes(Class<?> startClass) {
				final Class<?> superclass = startClass.getSuperclass();
				if (superclass == null)
					return Collections.emptyList();
				return Collections.singleton(superclass);
			}
		},
		INTERFACE_ONLY {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.util.reflect.ClassHierarchyVisiter.Mode#shouldInvokeCallback(java.lang.Class)
			 */
			@Override
			boolean shouldInvokeCallback(Class<?> superType) {
				return superType.isInterface();
			}
		},
		CLASS_FIRST {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.util.reflect.TypeHierarchyBrowser.Mode#prepare(java.util.Deque)
			 */
			@Override
			List<Class<?>> prepare(List<Class<?>> nextTypes) {
				Collections.sort(nextTypes, new Comparator<Class<?>>() {
					/*
					 * (non-Javadoc)
					 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
					 */
					@Override
					public int compare(Class<?> o1, Class<?> o2) {
						int interface1 = o1.isInterface() ? 1 : 0;
						int interface2 = o2.isInterface() ? 1 : 0;
						return interface1 - interface2;
					}
				});
				return nextTypes;
			}
		},
		INTERFACE_FIRST {
			@SuppressWarnings("unchecked")
			@Override
			Iterable<? extends Class<?>> getSuperTypes(Class<?> startClass) {
				final Class<?> superclass = startClass.getSuperclass();
				if (superclass == null)
					return Arrays.asList(startClass.getInterfaces());
				return new ConcatenatingIterable<Class<?>>(Arrays.asList(startClass.getInterfaces()),
					Collections.singleton(startClass.getSuperclass()));
			}

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.util.reflect.TypeHierarchyBrowser.Mode#prepare(java.util.Deque)
			 */
			@Override
			List<Class<?>> prepare(List<Class<?>> nextTypes) {
				Collections.sort(nextTypes, new Comparator<Class<?>>() {
					/*
					 * (non-Javadoc)
					 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
					 */
					@Override
					public int compare(Class<?> o1, Class<?> o2) {
						int interface1 = o1.isInterface() ? 0 : 1;
						int interface2 = o2.isInterface() ? 0 : 1;
						return interface1 - interface2;
					}
				});
				return nextTypes;
			}
		},
		ALL;

		boolean shouldInvokeCallback(@SuppressWarnings("unused") Class<?> superType) {
			return true;
		}

		@SuppressWarnings("unchecked")
		Iterable<? extends Class<?>> getSuperTypes(Class<?> startClass) {
			final Class<?> superclass = startClass.getSuperclass();
			if (superclass == null)
				return Arrays.asList(startClass.getInterfaces());
			return new ConcatenatingIterable<Class<?>>(Collections.singleton(startClass.getSuperclass()),
				Arrays.asList(startClass.getInterfaces()));
		}

		List<Class<?>> prepare(List<Class<?>> nextTypes) {
			return nextTypes;
		}
	}

	/**
	 * Moves through the hierarchy of the given start type and calls the callback for each type found up to the given
	 * depth.<br>
	 * The superclass and all directly implemented interfaces have a depth of one.<br>
	 * If the callback returns <code>false</code>, the method immediately returns.<br>
	 * Note that the same interface may be returned multiple times if it is implemented at more than one point in the
	 * hierarchy.
	 * 
	 * @param startType
	 *        the start type
	 * @param mode
	 *        the mode that determines what kinds of types are returned.
	 * @param callback
	 *        the callback to call
	 * @param maxDepth
	 *        the maximum depth
	 */
	public void visit(Class<?> startType, Mode mode, Visitor<Class<?>> callback, int maxDepth) {
		if (maxDepth <= 0)
			return;

		List<Class<?>> currentTypes = new LinkedList<Class<?>>(), nextTypes = new LinkedList<Class<?>>();
		currentTypes.add(startType);
		for (int depth = 1; depth <= maxDepth && !currentTypes.isEmpty(); depth++) {
			final boolean shouldDescend = depth + 1 <= maxDepth;

			for (Class<?> type : currentTypes) {
				Iterable<? extends Class<?>> superTypes = mode.getSuperTypes(type);
				for (Class<?> superType : superTypes) {
					if (mode.shouldInvokeCallback(superType)) {
						if (!callback.visited(superType, depth)) 						
							return;
					}
					if (shouldDescend)
						nextTypes.add(superType);
				}
			}

			currentTypes.clear();
			List<Class<?>> swap = currentTypes;
			currentTypes = mode.prepare(nextTypes);
			nextTypes = swap;
		}
	}

	/**
	 * Moves through the hierarchy of the given start type and calls the callback for each type found.<br>
	 * The superclass and all directly implemented interfaces have a depth of one.<br>
	 * If the callback returns <code>false</code>, the method immediately returns.<br>
	 * Note that the same interface may be returned multiple times if it is implemented at more than one point in the
	 * hierarchy.
	 * 
	 * @param startType
	 *        the start type
	 * @param mode
	 *        the mode that determines what kinds of types are returned.
	 * @param callback
	 *        the callback to call
	 */
	public void visit(Class<?> startType, Mode mode, Visitor<Class<?>> callback) {
		visit(startType, mode, callback, Integer.MAX_VALUE);
	}
}