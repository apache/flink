package eu.stratosphere.simple;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.base.Difference;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.Intersection;
import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.base.Union;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.util.reflect.ReflectUtil;

public class OperatorFactory {
	private Map<String, OperatorInfo> operators = new HashMap<String, OperatorFactory.OperatorInfo>();

	public static class OperatorInfo {

		private Class<? extends Operator> operatorClass;

		public OperatorInfo(Class<? extends Operator> operatorClass) {
			this.operatorClass = operatorClass;

			try {
				PropertyDescriptor[] propertyDescriptors = Introspector.getBeanInfo(operatorClass)
					.getPropertyDescriptors();
				for (PropertyDescriptor propertyDescriptor : propertyDescriptors)
					if (propertyDescriptor.getWriteMethod() != null)
						options.put(propertyDescriptor.getName(), propertyDescriptor);
			} catch (IntrospectionException e) {
				e.printStackTrace();
			}
		}

		private Map<String, PropertyDescriptor> options = new HashMap<String, PropertyDescriptor>();

		public void setProperty(String name, EvaluationExpression expression) {
			PropertyDescriptor propertyDescriptor = options.get(name);
			if (propertyDescriptor == null)
				throw new IllegalArgumentException("Unknown property: " + name);
		}

		public Operator newInstance(List<Operator> inputs) {
			try {
				Class<?>[] classes = new Class[inputs.size()];
				Arrays.fill(classes, JsonStream.class);
				return this.operatorClass.getConstructor(classes).newInstance(
					inputs.toArray(new JsonStream[inputs.size()]));
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	public OperatorFactory() {
		addOperator(Projection.class);
		addOperator(Selection.class);
		addOperator(Grouping.class);
		addOperator(Join.class);
		addOperator(Difference.class);
		addOperator(Union.class);
		addOperator(Intersection.class);
		addOperator(UnionAll.class);
	}

	public OperatorInfo getOperatorInfo(String operator) {
		return operators.get(operator.toLowerCase());
	}

	public void addOperator(Class<? extends Operator> operatorClass) {
		String name;
		Name nameAnnotation = ReflectUtil.getAnnotation(operatorClass, Name.class);
		if (nameAnnotation != null)
			name = operatorNameChooser.choose(nameAnnotation.noun(), nameAnnotation.verb(), nameAnnotation.adjective(),
				nameAnnotation.preposition());
		else
			name = operatorClass.getSimpleName().toLowerCase();
		operators.put(name, new OperatorInfo(operatorClass));
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
				String value = firstOrNull(names[pos]);
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
