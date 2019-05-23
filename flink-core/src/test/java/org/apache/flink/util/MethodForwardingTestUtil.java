/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.mockito.Mockito;
import org.mockito.internal.util.MockUtil;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

/**
 * Helper class with a method that attempts to automatically test method forwarding between a delegate and a wrapper.
 */
public class MethodForwardingTestUtil {

	/**
	 * This is a best effort automatic test for method forwarding between a delegate and its wrapper, where the wrapper
	 * class is a subtype of the delegate. This ignores methods that are inherited from Object.
	 *
	 * @param delegateClass the class for the delegate.
	 * @param wrapperFactory factory that produces a wrapper from a delegate.
	 * @param <D> type of the delegate
	 * @param <W> type of the wrapper
	 */
	public static <D, W> void testMethodForwarding(
		Class<D> delegateClass,
		Function<D, W> wrapperFactory)
		throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		testMethodForwarding(delegateClass, wrapperFactory, () -> spy(delegateClass), Collections.emptySet());
	}

	/**
	 * This is a best effort automatic test for method forwarding between a delegate and its wrapper, where the wrapper
	 * class is a subtype of the delegate. This ignores methods that are inherited from Object.
	 *
	 * @param delegateClass the class for the delegate.
	 * @param wrapperFactory factory that produces a wrapper from a delegate.
	 * @param delegateObjectSupplier supplier for the delegate object passed to the wrapper factory.
	 * @param <D> type of the delegate
	 * @param <W> type of the wrapper
	 * @param <I> type of the object created as delegate, is a subtype of D.
	 */
	public static <D, W, I extends D> void testMethodForwarding(
		Class<D> delegateClass,
		Function<I, W> wrapperFactory,
		Supplier<I> delegateObjectSupplier)
		throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		testMethodForwarding(delegateClass, wrapperFactory, delegateObjectSupplier, Collections.emptySet());
	}

	/**
	 * This is a best effort automatic test for method forwarding between a delegate and its wrapper, where the wrapper
	 * class is a subtype of the delegate. Methods can be remapped in case that the implementation does not call the
	 * original method. Remapping to null skips the method. This ignores methods that are inherited from Object.
	 *
	 * @param delegateClass the class for the delegate.
	 * @param wrapperFactory factory that produces a wrapper from a delegate.
	 * @param delegateObjectSupplier supplier for the delegate object passed to the wrapper factory.
	 * @param skipMethodSet set of methods to ignore.
	 * @param <D> type of the delegate
	 * @param <W> type of the wrapper
	 * @param <I> type of the object created as delegate, is a subtype of D.
	 */
	public static <D, W, I extends D> void testMethodForwarding(
		Class<D> delegateClass,
		Function<I, W> wrapperFactory,
		Supplier<I> delegateObjectSupplier,
		Set<Method> skipMethodSet) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

		Preconditions.checkNotNull(delegateClass);
		Preconditions.checkNotNull(wrapperFactory);
		Preconditions.checkNotNull(skipMethodSet);

		I delegate = delegateObjectSupplier.get();

		//check if we need to wrap the delegate object as a spy, or if it is already testable with Mockito.
		if (!MockUtil.isSpy(delegate) || !MockUtil.isMock(delegate)) {
			delegate = spy(delegate);
		}

		W wrapper = wrapperFactory.apply(delegate);

		// ensure that wrapper is a subtype of delegate
		Preconditions.checkArgument(delegateClass.isAssignableFrom(wrapper.getClass()));

		for (Method delegateMethod : delegateClass.getMethods()) {

			if (checkSkipMethodForwardCheck(delegateMethod, skipMethodSet)) {
				continue;
			}

			// find the correct method to substitute the bridge for erased generic types.
			// if this doesn't work, the user need to exclude the method and write an additional test.
			Method wrapperMethod = wrapper.getClass().getMethod(
				delegateMethod.getName(),
				delegateMethod.getParameterTypes());

			// things get a bit fuzzy here, best effort to find a match but this might end up with a wrong method.
			if (wrapperMethod.isBridge()) {
				for (Method method : wrapper.getClass().getMethods()) {
					if (!method.isBridge()
						&& method.getName().equals(wrapperMethod.getName())
						&& method.getParameterCount() == wrapperMethod.getParameterCount()) {
						wrapperMethod = method;
						break;
					}
				}
			}

			Class<?>[] parameterTypes = wrapperMethod.getParameterTypes();
			Object[] arguments = new Object[parameterTypes.length];
			for (int j = 0; j < arguments.length; j++) {
				Class<?> parameterType = parameterTypes[j];
				if (parameterType.isArray()) {
					arguments[j] = Array.newInstance(parameterType.getComponentType(), 0);
				} else if (parameterType.isPrimitive()) {
					if (boolean.class.equals(parameterType)) {
						arguments[j] = false;
					} else if (char.class.equals(parameterType)) {
						arguments[j] = 'a';
					} else {
						arguments[j] = (byte) 0;
					}
				} else {
					arguments[j] = Mockito.mock(parameterType);
				}
			}

			wrapperMethod.invoke(wrapper, arguments);
			delegateMethod.invoke(Mockito.verify(delegate, Mockito.times(1)), arguments);
			reset(delegate);
		}
	}

	/**
	 * Test if this method should be skipped in our check for proper forwarding, e.g. because it is just a bridge.
	 */
	private static boolean checkSkipMethodForwardCheck(Method delegateMethod, Set<Method> skipMethods) {

		if (delegateMethod.isBridge()
			|| delegateMethod.isDefault()
			|| skipMethods.contains(delegateMethod)) {
			return true;
		}

		// skip methods declared in Object (Mockito doesn't like them)
		try {
			Object.class.getMethod(delegateMethod.getName(), delegateMethod.getParameterTypes());
			return true;
		} catch (Exception ignore) {
		}
		return false;
	}
}
