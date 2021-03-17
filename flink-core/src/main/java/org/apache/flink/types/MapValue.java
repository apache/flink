/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.types;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.ReflectionUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Generic map base type for PACT programs that implements the Value and Map interfaces. The {@link
 * MapValue} encapsulates a Java {@link HashMap} object.
 *
 * @see org.apache.flink.types.Value
 * @see java.util.Map
 * @see java.util.HashMap
 * @param <K> Type of the map's key element.
 * @param <V> Type of the map's value element.
 */
@Public
public abstract class MapValue<K extends Value, V extends Value> implements Value, Map<K, V> {
    private static final long serialVersionUID = 1L;

    // type of the map's key
    private final Class<K> keyClass;
    // type of the map's value
    private final Class<V> valueClass;
    // encapsulated map
    private final Map<K, V> map;

    /** Initializes the encapsulated map with an empty HashMap. */
    public MapValue() {
        this.keyClass = ReflectionUtil.getTemplateType1(this.getClass());
        this.valueClass = ReflectionUtil.getTemplateType2(this.getClass());

        this.map = new HashMap<>();
    }

    /**
     * Initializes the encapsulated map with a HashMap filled with all entries of the provided map.
     *
     * @param map Map holding all entries with which the new encapsulated map is filled.
     */
    public MapValue(Map<K, V> map) {
        this.keyClass = ReflectionUtil.getTemplateType1(this.getClass());
        this.valueClass = ReflectionUtil.getTemplateType2(this.getClass());

        this.map = new HashMap<>(map);
    }

    @Override
    public void read(final DataInputView in) throws IOException {
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
        } catch (final InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(final DataOutputView out) throws IOException {
        out.writeInt(this.map.size());
        for (final Entry<K, V> entry : this.map.entrySet()) {
            entry.getKey().write(out);
            entry.getValue().write(out);
        }
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
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
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final MapValue<?, ?> other = (MapValue<?, ?>) obj;
        if (this.map == null) {
            if (other.map != null) {
                return false;
            }
        } else if (!this.map.equals(other.map)) {
            return false;
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * @see java.util.Map#clear()
     */
    @Override
    public void clear() {
        this.map.clear();
    }

    /*
     * (non-Javadoc)
     * @see java.util.Map#containsKey(java.lang.Object)
     */
    @Override
    public boolean containsKey(final Object key) {
        return this.map.containsKey(key);
    }

    /*
     * (non-Javadoc)
     * @see java.util.Map#containsValue(java.lang.Object)
     */
    @Override
    public boolean containsValue(final Object value) {
        return this.map.containsValue(value);
    }

    /*
     * (non-Javadoc)
     * @see java.util.Map#entrySet()
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        return this.map.entrySet();
    }

    /*
     * (non-Javadoc)
     * @see java.util.Map#get(java.lang.Object)
     */
    @Override
    public V get(final Object key) {
        return this.map.get(key);
    }

    /*
     * (non-Javadoc)
     * @see java.util.Map#isEmpty()
     */
    @Override
    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    /*
     * (non-Javadoc)
     * @see java.util.Map#keySet()
     */
    @Override
    public Set<K> keySet() {
        return this.map.keySet();
    }

    /*
     * (non-Javadoc)
     * @see java.util.Map#put(java.lang.Object, java.lang.Object)
     */
    @Override
    public V put(final K key, final V value) {
        return this.map.put(key, value);
    }

    /*
     * (non-Javadoc)
     * @see java.util.Map#putAll(java.util.Map)
     */
    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        this.map.putAll(m);
    }

    /*
     * (non-Javadoc)
     * @see java.util.Map#remove(java.lang.Object)
     */
    @Override
    public V remove(final Object key) {
        return this.map.remove(key);
    }

    /*
     * (non-Javadoc)
     * @see java.util.Map#size()
     */
    @Override
    public int size() {
        return this.map.size();
    }

    /*
     * (non-Javadoc)
     * @see java.util.Map#values()
     */
    @Override
    public Collection<V> values() {
        return this.map.values();
    }
}
