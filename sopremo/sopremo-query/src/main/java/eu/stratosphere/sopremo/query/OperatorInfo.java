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
package eu.stratosphere.sopremo.query;

import java.beans.IndexedPropertyDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.sopremo.operator.Operator;

public class OperatorInfo<Op extends Operator<Op>> extends InfoBase<Op> {

	public static class InputPropertyInfo extends PropertyInfo {

		/**
		 * Initializes OperatorPropertyInfo.
		 * 
		 * @param name
		 * @param descriptor
		 */
		protected InputPropertyInfo(final String name, final IndexedPropertyDescriptor descriptor) {
			super(name, descriptor);
		}

		@Override
		public IndexedPropertyDescriptor getDescriptor() {
			return (IndexedPropertyDescriptor) super.getDescriptor();
		}

		public Object getValue(final Operator<?> operator, final int index) {
			try {
				return this.getDescriptor().getIndexedReadMethod().invoke(operator, new Object[] { index });
			} catch (final Exception e) {
				throw new RuntimeException(
					String.format("Could not get property %s of %s", this.getName(), operator), e);
			}
		}

		public void setValue(final Operator<?> operator, final int index, final Object value) {
			try {
				this.getDescriptor().getIndexedWriteMethod().invoke(operator, index, value);
			} catch (final Exception e) {
				throw new RuntimeException(
					String.format("Could not set property %s of %s to %s", this.getName(), operator, value), e);
			}
		}

	}

	public static class OperatorPropertyInfo extends PropertyInfo {

		/**
		 * Initializes OperatorPropertyInfo.
		 * 
		 * @param name
		 * @param descriptor
		 */
		public OperatorPropertyInfo(final String name, final PropertyDescriptor descriptor) {
			super(name, descriptor);
		}

		public Object getValue(final Operator<?> operator) {
			try {
				return this.getDescriptor().getReadMethod().invoke(operator, new Object[0]);
			} catch (final Exception e) {
				throw new RuntimeException(
					String.format("Could not get property %s of %s", this.getName(), operator), e);
			}
		}

		public void setValue(final Operator<?> operator, final Object value) {
			try {
				this.getDescriptor().getWriteMethod().invoke(operator, value);
			} catch (final Exception e) {
				throw new RuntimeException(
					String.format("Could not set property %s of %s to %s", this.getName(), operator, value), e);
			}
		}

	}

	public static class PropertyInfo {
		private final String name;

		private final PropertyDescriptor descriptor;

		public PropertyInfo(final String name, final PropertyDescriptor descriptor) {
			this.name = name;
			this.descriptor = descriptor;
		}

		/**
		 * Returns the descriptor.
		 * 
		 * @return the descriptor
		 */
		public PropertyDescriptor getDescriptor() {
			return this.descriptor;
		}

		public boolean isFlag() {
			return this.getType() == Boolean.TYPE;
		}

		/**
		 * Returns the name.
		 * 
		 * @return the name
		 */
		public String getName() {
			return this.name;
		}

		/**
		 * @return
		 */
		public Class<?> getType() {
			return this.descriptor.getPropertyType();
		}

		@Override
		public String toString() {
			return this.name;
		}
	}

	Class<? extends Op> operatorClass;

	private final String name;

	private final NameChooser propertyNameChooser;

	private final Map<String, OperatorPropertyInfo> operatorProperties = new HashMap<String, OperatorPropertyInfo>();

	private final Map<String, InputPropertyInfo> inputProperties = new HashMap<String, InputPropertyInfo>();

	public OperatorInfo(final Class<? extends Op> operatorClass, final String opName,
			final NameChooser propertyNameChooser) {
		this.operatorClass = operatorClass;
		this.name = opName;
		this.propertyNameChooser = propertyNameChooser;
	}

	private void initProperties() {
		try {
			final PropertyDescriptor[] propertyDescriptors = Introspector.getBeanInfo(operatorClass)
				.getPropertyDescriptors();
			for (final PropertyDescriptor propertyDescriptor : propertyDescriptors)
				if (propertyDescriptor.getWriteMethod() != null
					|| propertyDescriptor instanceof IndexedPropertyDescriptor &&
					((IndexedPropertyDescriptor) propertyDescriptor).getIndexedWriteMethod() != null) {
					String name = propertyNameChooser.choose(
						(String[]) propertyDescriptor.getValue(Operator.Info.NAME_NOUNS),
						(String[]) propertyDescriptor.getValue(Operator.Info.NAME_VERB),
						(String[]) propertyDescriptor.getValue(Operator.Info.NAME_ADJECTIVE),
						(String[]) propertyDescriptor.getValue(Operator.Info.NAME_PREPOSITION));
					if (name == null)
						name = propertyDescriptor.getName();

					if (propertyDescriptor.getValue(Operator.Info.INPUT) == Boolean.TRUE)
						this.inputProperties.put(name, new InputPropertyInfo(name,
							(IndexedPropertyDescriptor) propertyDescriptor));
					else
						this.operatorProperties.put(name, new OperatorPropertyInfo(name, propertyDescriptor));
				}
		} catch (final IntrospectionException e) {
			e.printStackTrace();
		}
	}

	public Map<String, InputPropertyInfo> getInputProperties() {
		if (needsInitialization())
			initProperties();
		return this.inputProperties;
	}

	public InputPropertyInfo getInputPropertyInfo(final String name) {
		return getInputProperties().get(name);
	}

	public String getName() {
		return this.name;
	}

	public Map<String, OperatorPropertyInfo> getOperatorProperties() {
		if (needsInitialization())
			initProperties();
		return this.operatorProperties;
	}

	public OperatorPropertyInfo getOperatorProperty(final String name) {
		return getOperatorProperties().get(name);
	}

	public Operator<Op> newInstance() {
		try {
			return this.operatorClass.newInstance();
		} catch (final InstantiationException e) {
			e.printStackTrace();
		} catch (final IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String toString() {
		return this.name;
	}
}