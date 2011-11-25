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
package eu.stratosphere.sopremo;

import java.beans.IndexedPropertyDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.sopremo.OperatorFactory.NameChooser;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class OperatorInfo<Op extends Operator<Op>> {

	Class<? extends Op> operatorClass;

	private String name;

	public OperatorInfo(Class<? extends Op> operatorClass, String opName, NameChooser propertyNameChooser) {
		this.operatorClass = operatorClass;
		this.name = opName;

		try {
			PropertyDescriptor[] propertyDescriptors = Introspector.getBeanInfo(operatorClass)
				.getPropertyDescriptors();
			for (PropertyDescriptor propertyDescriptor : propertyDescriptors)
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
						this.inputProperties.put(name, new OperatorPropertyInfo(name, propertyDescriptor));
					else
						this.operatorProperties.put(name, new OperatorPropertyInfo(name, propertyDescriptor));
				}
		} catch (IntrospectionException e) {
			e.printStackTrace();
		}
	}

	public String getName() {
		return this.name;
	}

	private Map<String, OperatorPropertyInfo> operatorProperties = new HashMap<String, OperatorPropertyInfo>();

	private Map<String, OperatorPropertyInfo> inputProperties = new HashMap<String, OperatorPropertyInfo>();

	public boolean hasProperty(String name) {
		return this.operatorProperties.get(name) != null;
	}

	public Object getProperty(String name, Operator<Op> operator) {
		return getPropertySafely(name, operator).getValue(operator);
	}

	public void setProperty(String name, Operator<Op> operator, Object value) {
		getPropertySafely(name, operator).setValue(operator, value);
	}

	protected OperatorPropertyInfo getPropertySafely(String name, Operator<Op> operator) {
		return getTypedPropertySafely(name, operator, false);
	}

	protected OperatorPropertyInfo getTypedPropertySafely(String name, Operator<Op> operator, boolean input) {
		Map<String, OperatorPropertyInfo> properties = input ? this.inputProperties : this.operatorProperties;
		OperatorPropertyInfo property = properties.get(name);
		if (property == null)
			throw new IllegalArgumentException(String.format("Unknown property %s for operator %s (available %s)",
				name, operator.getName(), properties));
		return property;
	}

	protected OperatorPropertyInfo getIndexPropertySafely(String name, Operator<Op> operator) {
		return getTypedPropertySafely(name, operator, true);
	}

	public boolean hasInputProperty(String name) {
		return this.inputProperties.get(name) != null;
	}

	public boolean hasFlag(String name) {
		return this.operatorProperties.get(name) != null
			&& this.operatorProperties.get(name).getType() == Boolean.TYPE;
	}

	public Map<String, OperatorPropertyInfo> getOperatorProperties() {
		return this.operatorProperties;
	}

	public Map<String, OperatorPropertyInfo> getInputProperties() {
		return this.inputProperties;
	}

	public void setInputProperty(String name, Operator<Op> operator, int inputIndex, EvaluationExpression expression) {
		getIndexPropertySafely(name, operator).setValue(operator, inputIndex, expression);
		try {
			((IndexedPropertyDescriptor) propertyDescriptor).getIndexedWriteMethod().invoke(operator, inputIndex,
				expression);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}

	public Object getInputProperty(String name, Operator<Op> operator, int inputIndex) {
		PropertyDescriptor propertyDescriptor = this.inputProperties.get(name);
		if (propertyDescriptor == null)
			throw new IllegalArgumentException(String.format(
				"Unknown input property %s for operator %s (available %s)",
				name, operator.getName(), this.inputProperties.keySet()));
		try {
			return ((IndexedPropertyDescriptor) propertyDescriptor).getIndexedReadMethod().invoke(operator,
				new Object[] { inputIndex });
		} catch (Exception e) {
			throw new RuntimeException(
				String.format("Could not get property %s of %s", name, operator), e);
		}
	}

	public Operator<Op> newInstance() {
		try {
			return this.operatorClass.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String toString() {
		return this.name;
	}

	public static class PropertyInfo {
		private String name;

		private PropertyDescriptor descriptor;

		public PropertyInfo(String name, PropertyDescriptor descriptor) {
			this.name = name;
			this.descriptor = descriptor;
		}

		/**
		 * @return
		 */
		public Class<?> getType() {
			return descriptor.getPropertyType();
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
		 * Returns the descriptor.
		 * 
		 * @return the descriptor
		 */
		public PropertyDescriptor getDescriptor() {
			return this.descriptor;
		}
	}

	public static class OperatorPropertyInfo extends PropertyInfo {

		/**
		 * Initializes OperatorPropertyInfo.
		 *
		 * @param name
		 * @param descriptor
		 */
		public OperatorPropertyInfo(String name, PropertyDescriptor descriptor) {
			super(name, descriptor);
		}

		public Object getValue(Operator<?> operator) {
			try {
				return getDescriptor().getReadMethod().invoke(operator, new Object[0]);
			} catch (Exception e) {
				throw new RuntimeException(
					String.format("Could not get property %s of %s", getName(), operator), e);
			}
		}

		public void setValue(Operator<?> operator, Object value) {
			try {
				getDescriptor().getWriteMethod().invoke(operator, value);
			} catch (Exception e) {
				throw new RuntimeException(
					String.format("Could not set property %s of %s to %s", getName(), operator, value), e);
			}
		}

	}
}