package eu.stratosphere.sopremo;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import eu.stratosphere.sopremo.expressions.MethodPointerExpression;
import eu.stratosphere.sopremo.function.JsonMethod;

public class Bindings extends AbstractSopremoType implements SerializableSopremoType {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5361273437492630123L;

	private final LinkedList<Map<String, Object>> bindings = new LinkedList<Map<String, Object>>();

	public static enum BindingConstraint {
		NON_NULL {
			@Override
			public Object process(final Object input, final Class<?> expectedType) {
				if (input == null)
					throw new IllegalArgumentException("Unknown variable");
				return input;
			}
		},
		AUTO_FUNCTION_POINTER {
			@Override
			public Object process(final Object input, final Class<?> expectedType) {
				if (!expectedType.isInstance(input) && input instanceof JsonMethod)
					return new MethodPointerExpression(((JsonMethod) input).getName());
				return input;
			}
		};

		public abstract Object process(Object input, Class<?> expectedType);
	}

	public Bindings() {
		this.addScope();
	}

	public void addScope() {
		this.bindings.addLast(new HashMap<String, Object>());
	}

	public Object get(final String name) {
		final Iterator<Map<String, Object>> iterator = this.bindings.descendingIterator();
		while (iterator.hasNext()) {
			final Object binding = iterator.next().get(name);
			if (binding != null)
				return binding;
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public <T> T get(final String name, final Class<T> expectedType, final BindingConstraint... constraints) {
		Object value = this.get(name);

		for (final BindingConstraint contraint : constraints)
			value = contraint.process(value, expectedType);

		if (value == null)
			return null;
		if (!expectedType.isInstance(value))
			throw new IllegalArgumentException(String.format("Variable %s has unexpected type %s; should have been %s",
				name,
				value.getClass().getSimpleName(), expectedType.getSimpleName()));
		return (T) value;
	}

	public Map<String, Object> getAll() {
		return this.getAll(Object.class);
	}

	@SuppressWarnings("unchecked")
	public <T> Map<String, T> getAll(final Class<T> expectedType) {
		final Map<String, T> combinedBindings = new HashMap<String, T>();
		final Iterator<Map<String, Object>> iterator = this.bindings.descendingIterator();
		while (iterator.hasNext())
			for (final Map.Entry<String, Object> binding : iterator.next().entrySet())
				if (!combinedBindings.containsKey(binding.getKey()) && expectedType.isInstance(binding.getValue()))
					combinedBindings.put(binding.getKey(), (T) binding.getValue());
		return combinedBindings;
	}

	public void removeScope() {
		this.bindings.removeLast();
	}

	public void set(final String name, final Object binding) {
		this.set(name, binding, 0);
	}

	public void set(final String name, final Object binding, final int scopeLevel) {
		this.bindings.get(this.bindings.size() - 1 - scopeLevel).put(name, binding);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.SopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(final StringBuilder builder) {
		builder.append(this.getAll().toString());
	}

	public void putAll(final Bindings bindings) {
		this.bindings.addAll(bindings.bindings);
	}
}
