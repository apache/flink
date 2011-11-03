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
	private PropertyDescriptor propertyDescriptor;

	public BeanProperty(PropertyDescriptor propertyDescriptor) {
		this.propertyDescriptor = propertyDescriptor;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.reflect.DynamicProperty#set(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void set(Object instance, Type value) {
		try {
			this.propertyDescriptor.getWriteMethod().invoke(instance, value);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.reflect.DynamicProperty#get(java.lang.Object)
	 */
	@Override
	public Type get(Object instance) {
		try {
			return (Type) this.propertyDescriptor.getReadMethod().invoke(instance);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.reflect.DynamicProperty#getType()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Class<Type> getType() {
		return (Class) this.propertyDescriptor.getPropertyType();
	}
}
