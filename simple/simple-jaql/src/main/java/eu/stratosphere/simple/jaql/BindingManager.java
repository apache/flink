package eu.stratosphere.simple.jaql;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import eu.stratosphere.simple.jaql.QueryParser.Binding;

class BindingManager {

	private LinkedList<Map<String, Binding>> bindings = new LinkedList<Map<String, Binding>>();

	public BindingManager() {
	}

	public void addScope() {
		this.bindings.addLast(new HashMap<String, Binding>());
	}

	public Binding get(String name) {
		Iterator<Map<String, Binding>> iterator = this.bindings.descendingIterator();
		while (iterator.hasNext()) {
			Binding binding = iterator.next().get(name);
			if (binding != null)
				// System.out.println("get " + name + " -> " + binding);
				return binding;
		}
		return null;
	}

	public Map<String, Binding> getAll() {
		return this.bindings.getLast();
	}

	private LinkedHashMap<String, Object> getCombinedBindings() {
		LinkedHashMap<String, Object> combinedBindings = new LinkedHashMap<String, Object>();

		Iterator<Map<String, Binding>> iterator = this.bindings.descendingIterator();
		while (iterator.hasNext())
			for (Map.Entry<String, Binding> binding : iterator.next().entrySet())
				if (!combinedBindings.containsKey(binding.getKey()))
					combinedBindings.put(binding.getKey(), binding.getValue());
		return combinedBindings;
	}

	public void removeScope() {
		this.bindings.removeLast();
	}

	public void set(String name, Binding binding) {
		// System.out.println("set " + name + " -> " + binding + " with transformation "
		// + binding.getTransformed().getClass().getSimpleName() + " replacing "
		// + getAll().get(name));
		this.bindings.getLast().put(name, binding);
	}

	@Override
	public String toString() {
		return this.getCombinedBindings().toString();
	}
}
