package eu.stratosphere.util;

import java.util.AbstractSet;
import java.util.IdentityHashMap;
import java.util.Iterator;

public class IdentitySet<E> extends AbstractSet<E> {
	private IdentityHashMap<E, Object> backing = new IdentityHashMap<E, Object>();

	@Override
	public boolean add(E e) {
		return this.backing.put(e, null) == null;
	}

	@Override
	public void clear() {
		this.backing.clear();
	}

	@Override
	public boolean contains(Object o) {
		return this.backing.containsKey(o);
	};

	@Override
	public Iterator<E> iterator() {
		return this.backing.keySet().iterator();
	}

	@Override
	public boolean remove(Object o) {
		return this.backing.keySet().remove(o);
	}

	@Override
	public int size() {
		return this.backing.size();
	}
}
