package eu.stratosphere.sopremo.query;

import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import eu.stratosphere.sopremo.packages.IRegistry;

public class StackedRegistry<T, R extends IRegistry<T>> implements IRegistry<T>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 5126957012101353155L;
	private LinkedList<R> registryStack = new LinkedList<R>();
	
	public StackedRegistry(R topRegistry) {
		registryStack.add(topRegistry);
	}
	
	@Override
	public T get(String name) {
		for(R registry : registryStack) {
			T element = registry.get(name);
			if(element != null)return element;
		}
		return null;
	}
	
	public void push(R e) {
		this.registryStack.add(1, e);
	}

	public R pop() {
		return this.registryStack.remove(1);
	}

	@Override
	public Set<String> keySet() {
		final HashSet<String> keys = new HashSet<String>();
		for(R registry : registryStack) 
			keys.addAll(registry.keySet());
		return keys;
	}
	
	public void put(String name, T element) {
		getTopRegistry().put(name, element);
	}

	protected R getTopRegistry() {
		return this.registryStack.peek();
	}

	@Override
	public void toString(StringBuilder builder) {
		builder.append("Registry with ");
		for(R registry : registryStack)  {
			builder.append("\n ");
			registry.toString(builder);
		}
	}
}
