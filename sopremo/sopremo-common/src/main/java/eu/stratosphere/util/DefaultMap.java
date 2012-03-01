package eu.stratosphere.util;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A map implementation that returns a default value instead of <code>null</code>.
 * 
 * @author Arvid Heise
 * @param <K>
 *        the key
 * @param <V>
 *        the value
 */
public class DefaultMap<K, V> extends AbstractMap<K, V> {
	private V defaultValue;

	private Map<K, V> backing;

	public DefaultMap(final V defaultValue) {
		this(defaultValue, new HashMap<K, V>());
	}

	public DefaultMap(final V defaultValue, final Map<K, V> backing) {
		this.defaultValue = defaultValue;
		this.backing = backing;
	}

	@Override
	public void clear() {
		this.backing.clear();
	}

	@Override
	public boolean containsKey(final Object key) {
		return this.backing.containsKey(key);
	}

	@Override
	public boolean containsValue(final Object value) {
		return this.backing.containsValue(value);
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return this.backing.entrySet();
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		@SuppressWarnings("unchecked")
		final DefaultMap<K, V> other = (DefaultMap<K, V>) obj;
		return this.defaultValue.equals(other.defaultValue) && this.backing.equals(other.backing);
	}

	/**
	 * Returns the default value if the given key is not in the map.<br>
	 * Otherwise the original semantics of {@link Map#get(Object)} is maintained.
	 * 
	 * @return the value to which the specified key is mapped, or
	 *         the default value if this map contains no mapping for the key
	 */
	@Override
	public V get(final Object key) {
		final V value = this.backing.get(key);
		return value == null ? this.defaultValue : value;
	}

	public V getDefaultValue() {
		return this.defaultValue;
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
	public boolean isEmpty() {
		return this.backing.isEmpty();
	}

	@Override
	public Set<K> keySet() {
		return this.backing.keySet();
	}

	@Override
	public V put(final K key, final V value) {
		return this.backing.put(key, value);
	}

	@Override
	public void putAll(final Map<? extends K, ? extends V> m) {
		this.backing.putAll(m);
	}

	@Override
	public V remove(final Object key) {
		return this.backing.remove(key);
	}

	public void setDefaultValue(final V defaultValue) {
		if (defaultValue == null)
			throw new NullPointerException("defaultValue must not be null");

		this.defaultValue = defaultValue;
	}

	@Override
	public int size() {
		return this.backing.size();
	}

	@Override
	public Collection<V> values() {
		return this.backing.values();
	}
}
