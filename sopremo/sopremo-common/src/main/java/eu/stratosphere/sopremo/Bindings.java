package eu.stratosphere.sopremo;

import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import eu.stratosphere.sopremo.expressions.MethodPointerExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.MethodCall;
import eu.stratosphere.sopremo.function.JsonMethod;

public class Bindings implements SerializableSopremoType {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5361273437492630123L;

	private LinkedList<Map<String, Object>> bindings = new LinkedList<Map<String, Object>>();

	public static enum BindingConstraint {
		NON_NULL {
			@Override
			public Object process(Object input, Class<?> expectedType) {
				if (input == null)
					throw new IllegalArgumentException("Unknown variable");
				return input;
			}
		},
		AUTO_FUNCTION_POINTER {
			@Override
			public Object process(Object input, Class<?> expectedType) {
				if (!expectedType.isInstance(input) && input instanceof JsonMethod)
					return new MethodPointerExpression(((JsonMethod) input).getName());
				return input;
			}
		};

		public abstract Object process(Object input, Class<?> expectedType);
	}

	public Bindings() {
		addScope();
	}

	public void addScope() {
		this.bindings.addLast(new HashMap<String, Object>());
	}

	public Object get(String name) {
		Iterator<Map<String, Object>> iterator = this.bindings.descendingIterator();
		while (iterator.hasNext()) {
			Object binding = iterator.next().get(name);
			if (binding != null)
				return binding;
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public <T> T get(String name, Class<T> expectedType, BindingConstraint... constraints) {
		Object value = get(name);
		
		for (BindingConstraint contraint : constraints) 
			value = contraint.process(value, expectedType);
		
		if (value == null)
			return null;
		if (!expectedType.isInstance(value))
			throw new IllegalArgumentException(String.format("Variable %s has unexpected type %s; should have been %s", name,
				value.getClass().getSimpleName(), expectedType.getSimpleName()));
		return (T) value;
	}

	public Map<String, Object> getAll() {
		return getAll(Object.class);
	}

	@SuppressWarnings("unchecked")
	public <T> Map<String, T> getAll(Class<T> expectedType) {
		Map<String, T> combinedBindings = new HashMap<String, T>();
		Iterator<Map<String, Object>> iterator = this.bindings.descendingIterator();
		while (iterator.hasNext())
			for (Map.Entry<String, Object> binding : iterator.next().entrySet())
				if (!combinedBindings.containsKey(binding.getKey()) && expectedType.isInstance(binding.getValue()))
					combinedBindings.put(binding.getKey(), (T) binding.getValue());
		return combinedBindings;
	}

	public void removeScope() {
		this.bindings.removeLast();
	}

	public void set(String name, Object binding) {
		set(name, binding, 0);
	}

	public void set(String name, Object binding, int scopeLevel) {
		this.bindings.get(this.bindings.size() - 1 - scopeLevel).put(name, binding);
	}

	@Override
	public String toString() {
		return this.getAll().toString();
	}
}
