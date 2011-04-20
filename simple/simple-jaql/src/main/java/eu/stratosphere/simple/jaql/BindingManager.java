package eu.stratosphere.simple.jaql;

import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import eu.stratosphere.simple.jaql.JaqlPlanCreator.Binding;

public class BindingManager {

	private LinkedList<Map<String, Binding>> bindings = new LinkedList<Map<String, Binding>>();

	public BindingManager() {
	}

	public void set(String name, Binding binding) {
//		System.out.println("set " + name + " -> " + binding + " with transformation "
//			+ binding.getTransformed().getClass().getSimpleName() + " replacing "
//			+ getAll().get(name));
		bindings.getLast().put(name, binding);
	}

	public Map<String, Binding> getAll() {
		return bindings.getLast();
	}

	public Binding get(String name) {
		Iterator<Map<String, Binding>> iterator = bindings.descendingIterator();
		while (iterator.hasNext()) {
			Binding binding = iterator.next().get(name);
			if (binding != null) {
//				System.out.println("get " + name + " -> " + binding);
				return binding;
			}
		}
		return null;
	}

	public void addScope() {
		bindings.addLast(new HashMap<String, Binding>());
	}

	public void removeScope() {
		bindings.removeLast();
	}

	@Override
	public String toString() {
		return getCombinedBindings().toString();
	}

	private LinkedHashMap<String, Object> getCombinedBindings() {
		LinkedHashMap<String, Object> combinedBindings = new LinkedHashMap<String, Object>();

		Iterator<Map<String, Binding>> iterator = bindings.descendingIterator();
		while (iterator.hasNext()) {
			for (Map.Entry<String, Binding> binding : iterator.next().entrySet()) {
				if (!combinedBindings.containsKey(binding.getKey())) {
					combinedBindings.put(binding.getKey(), binding.getValue());
				}
			}
		}
		return combinedBindings;
	}
}
