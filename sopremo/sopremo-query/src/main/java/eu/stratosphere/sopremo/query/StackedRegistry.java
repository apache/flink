package eu.stratosphere.sopremo.query;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import eu.stratosphere.sopremo.packages.IRegistry;

public class StackedRegistry<T, R extends IRegistry<T>> implements IRegistry<T> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5126957012101353155L;

	private LinkedList<R> registryStack = new LinkedList<R>();

	public StackedRegistry(R topRegistry) {
		this.registryStack.add(topRegistry);
	}

	@Override
	public T get(String name) {
		for (R registry : this.registryStack) {
			T element = registry.get(name);
			if (element != null)
				return element;
		}
		return null;
	}

	public void push(R e) {
		this.registryStack.push(e);
	}

	public R pop() {
		return this.registryStack.pop();
	}

	/**
	 * Returns the registryStack.
	 * 
	 * @return the registryStack
	 */
	public R getRegistry(int level) {
		return this.registryStack.get(level);
	}

	@Override
	public Set<String> keySet() {
		final HashSet<String> keys = new HashSet<String>();
		for (R registry : this.registryStack)
			keys.addAll(registry.keySet());
		return keys;
	}

	@Override
	public void put(String name, T element) {
		this.getTopRegistry().put(name, element);
	}

	protected R getTopRegistry() {
		return this.registryStack.peek();
	}

	@Override
	public void toString(StringBuilder builder) {
		builder.append("Registry with ");
		for (R registry : this.registryStack) {
			builder.append("\n ");
			registry.toString(builder);
		}
	}
}
