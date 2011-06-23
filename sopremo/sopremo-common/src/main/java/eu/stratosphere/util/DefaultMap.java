package eu.stratosphere.util;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DefaultMap<K, V> extends AbstractMap<K, V> {
	private V defaultValue;

	private Map<K, V> backing;

	public V getDefaultValue() {
		return this.defaultValue;
	}

	public void setDefaultValue(V defaultValue) {
		if (defaultValue == null)
			throw new NullPointerException("defaultValue must not be null");

		this.defaultValue = defaultValue;
	}

	public DefaultMap(V defaultValue, Map<K, V> backing) {
		this.defaultValue = defaultValue;
		this.backing = backing;
	}

	public DefaultMap(V defaultValue) {
		this(defaultValue, new HashMap<K, V>());
	}

	@Override
	public int size() {
		return this.backing.size();
	}

	@Override
	public boolean isEmpty() {
		return this.backing.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return this.backing.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return this.backing.containsValue(value);
	}

	@Override
	public V get(Object key) {
		V value = this.backing.get(key);
		return value == null ? this.defaultValue : value;
	}

	@Override
	public V put(K key, V value) {
		return this.backing.put(key, value);
	}

	@Override
	public V remove(Object key) {
		return this.backing.remove(key);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		this.backing.putAll(m);
	}

	@Override
	public void clear() {
		this.backing.clear();
	}

	@Override
	public Set<K> keySet() {
		return this.backing.keySet();
	}

	@Override
	public Collection<V> values() {
		return this.backing.values();
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return this.backing.entrySet();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.backing.hashCode();
		result = prime * result + this.defaultValue.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		@SuppressWarnings("unchecked")
		DefaultMap<K, V> other = (DefaultMap<K, V>) obj;
		return this.defaultValue.equals(other.defaultValue) && this.backing.equals(other.backing);
	}

}
