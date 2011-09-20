package eu.stratosphere.sopremo;

import java.beans.IndexedPropertyDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.util.reflect.ReflectUtil;

public class OperatorFactory {
	private Map<String, OperatorInfo<?>> operators = new HashMap<String, OperatorInfo<?>>();

	public class OperatorInfo<Op extends Operator<Op>> {

		private Class<? extends Op> operatorClass;

		public OperatorInfo(Class<? extends Op> operatorClass) {
			this.operatorClass = operatorClass;

			try {
				PropertyDescriptor[] propertyDescriptors = Introspector.getBeanInfo(operatorClass)
					.getPropertyDescriptors();
				for (PropertyDescriptor propertyDescriptor : propertyDescriptors)
					if (propertyDescriptor.getWriteMethod() != null
						|| propertyDescriptor instanceof IndexedPropertyDescriptor &&
						((IndexedPropertyDescriptor) propertyDescriptor).getIndexedWriteMethod() != null) {
						String name = OperatorFactory.this.propertyNameChooser.choose(
							(String[]) propertyDescriptor.getValue(Operator.Info.NAME_NOUNS),
							(String[]) propertyDescriptor.getValue(Operator.Info.NAME_VERB),
							(String[]) propertyDescriptor.getValue(Operator.Info.NAME_ADJECTIVE),
							(String[]) propertyDescriptor.getValue(Operator.Info.NAME_PREPOSITION));
						if (name == null)
							name = propertyDescriptor.getName();

						if (propertyDescriptor.getValue(Operator.Info.INPUT) == Boolean.TRUE)
							this.inputProperties.put(name, propertyDescriptor);
						else
							this.operatorProperties.put(name, propertyDescriptor);
					}
			} catch (IntrospectionException e) {
				e.printStackTrace();
			}
		}

		private Map<String, PropertyDescriptor> operatorProperties = new HashMap<String, PropertyDescriptor>();

		private Map<String, PropertyDescriptor> inputProperties = new HashMap<String, PropertyDescriptor>();

		public boolean hasProperty(String name) {
			return this.operatorProperties.get(name) != null;
		}

		public void setProperty(String name, Op operator, EvaluationExpression expression) {
			PropertyDescriptor propertyDescriptor = this.operatorProperties.get(name);
			if (propertyDescriptor == null)
				throw new IllegalArgumentException(String.format("Unknown property %s for operator %s (available %s)",
					name, operator.getName(), this.operatorProperties.keySet()));
			try {
				propertyDescriptor.getWriteMethod().invoke(operator, expression);
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
		}

		public boolean hasInputProperty(String name) {
			return this.inputProperties.get(name) != null;
		}

		public void setInputProperty(String name, Op operator, int inputIndex, EvaluationExpression expression) {
			PropertyDescriptor propertyDescriptor = this.inputProperties.get(name);
			if (propertyDescriptor == null)
				throw new IllegalArgumentException(String.format("Unknown property %s for operator %s (available %s)",
					name, operator.getName(), this.inputProperties.keySet()));
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

		public Op newInstance() {
			try {
				return this.operatorClass.newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	public OperatorFactory() {
	}

	public OperatorInfo<?> getOperatorInfo(String operator) {
		return this.operators.get(operator.toLowerCase());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void addOperator(Class<? extends Operator<?>> operatorClass) {
		String name;
		Name nameAnnotation = ReflectUtil.getAnnotation(operatorClass, Name.class);
		if (nameAnnotation != null)
			name = this.operatorNameChooser.choose(nameAnnotation.noun(), nameAnnotation.verb(), nameAnnotation.adjective(),
				nameAnnotation.preposition());
		else
			name = operatorClass.getSimpleName().toLowerCase();
		this.operators.put(name, new OperatorInfo(operatorClass));
	}

	public static interface NameChooser {
		public String choose(String[] nouns, String[] verbs, String[] adjectives, String[] prepositions);
	}

	private NameChooser operatorNameChooser = new DefaultNameChooser();

	private NameChooser propertyNameChooser = new DefaultNameChooser();

	public static class DefaultNameChooser implements NameChooser {
		private int[] preferredOrder;

		public DefaultNameChooser(int... preferredOrder) {
			this.preferredOrder = preferredOrder;
		}

		public DefaultNameChooser() {
			this(3, 0, 1, 2);
		}

		@Override
		public String choose(String[] nouns, String[] verbs, String[] adjectives, String[] prepositions) {
			String[][] names = { nouns, verbs, adjectives, prepositions };
			for (int pos : this.preferredOrder) {
				String value = this.firstOrNull(names[pos]);
				if (value != null)
					return value;
			}
			return null;
		}

		private String firstOrNull(String[] names) {
			return names.length == 0 ? null : names[0];
		}
	}
}
