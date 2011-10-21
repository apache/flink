package eu.stratosphere.sopremo;

import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

public class Bindings {
	private Deque<Map<String, Object>> bindings = new LinkedList<Map<String, Object>>();

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
	public <T> T get(String name, Class<T> expectedType) {
		Object value = get(name);
		if (value == null)
			return null;
		if (!expectedType.isInstance(value))
			throw new IllegalArgumentException(String.format("Variable %s has unexpected type %s", name,
				value.getClass()));
		return (T) value;
	}

	public <T> T getNonNull(String name, Class<T> expectedType) {
		T value = get(name, expectedType);
		if (value == null)
			throw new IllegalArgumentException("Unknown variable " + name);
		return value;
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
		this.bindings.getLast().put(name, binding);
	}

	@Override
	public String toString() {
		return this.getAll().toString();
	}
}
