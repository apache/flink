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

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Arvid Heise
 */
public class BeanProperty<Type> extends DynamicProperty<Type> {
	private final PropertyDescriptor propertyDescriptor;

	public BeanProperty(final PropertyDescriptor propertyDescriptor) {
		this.propertyDescriptor = propertyDescriptor;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.reflect.DynamicProperty#set(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void set(final Object instance, final Type value) {
		try {
			this.propertyDescriptor.getWriteMethod().invoke(instance, value);
		} catch (final IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (final InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.reflect.DynamicProperty#get(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Type get(final Object instance) {
		try {
			return (Type) this.propertyDescriptor.getReadMethod().invoke(instance);
		} catch (final IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (final InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.reflect.DynamicProperty#getType()
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Class<Type> getType() {
		return (Class) this.propertyDescriptor.getPropertyType();
	}
}
