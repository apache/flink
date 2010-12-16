/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.common.type.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.ReflectionUtil;

public abstract class PactMap<K extends Value, V extends Value> implements Value, Map<K, V> {
	private final Class<K> keyClass;

	private final Class<V> valueClass;

	private final Map<K, V> map;

	public PactMap() {
		this.keyClass = ReflectionUtil.<K> getTemplateType1(this.getClass());
		this.valueClass = ReflectionUtil.<V> getTemplateType2(this.getClass());

		this.map = new HashMap<K, V>();
	}

	public PactMap(Map<K, V> map) {
		this.keyClass = ReflectionUtil.<K> getTemplateType1(this.getClass());
		this.valueClass = ReflectionUtil.<V> getTemplateType2(this.getClass());

		this.map = new HashMap<K, V>(map);
	}

	@Override
	public void read(final DataInput in) throws IOException {
		int size = in.readInt();
		this.map.clear();

		try {
			for (; size > 0; size--) {
				final K key = this.keyClass.newInstance();
				final V val = this.valueClass.newInstance();
				key.read(in);
				val.read(in);
				this.map.put(key, val);
			}
		} catch (final InstantiationException e) {
			throw new RuntimeException(e);
		} catch (final IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeInt(this.map.size());
		for (final Entry<K, V> entry : this.map.entrySet()) {
			entry.getKey().write(out);
			entry.getValue().write(out);
		}
	}

	@Override
	public String toString() {
		return this.map.toString();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 47;
		int result = 1;
		result = prime * result + this.map.hashCode();
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final PactMap<?, ?> other = (PactMap<?, ?>) obj;
		if (this.map == null) {
			if (other.map != null)
				return false;
		} else if (!this.map.equals(other.map))
			return false;
		return true;
	}

	public void clear() {
		this.map.clear();
	}

	public boolean containsKey(final Object key) {
		return this.map.containsKey(key);
	}

	public boolean containsValue(final Object value) {
		return this.map.containsValue(value);
	}

	public Set<Entry<K, V>> entrySet() {
		return this.map.entrySet();
	}

	public V get(final Object key) {
		return this.map.get(key);
	}

	public boolean isEmpty() {
		return this.map.isEmpty();
	}

	public Set<K> keySet() {
		return this.map.keySet();
	}

	public V put(final K key, final V value) {
		return this.map.put(key, value);
	}

	public void putAll(final Map<? extends K, ? extends V> m) {
		this.map.putAll(m);
	}

	public V remove(final Object key) {
		return this.map.remove(key);
	}

	public int size() {
		return this.map.size();
	}

	public Collection<V> values() {
		return this.map.values();
	}

}
