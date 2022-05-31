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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.Objects;

/** A {@link Comparable} holder for key-value pairs. */
public class KeyValue<K extends Comparable<? super K>, V extends Comparable<? super V>>
        implements Comparable<KeyValue<K, V>>, Serializable {

    private static final long serialVersionUID = 1L;

    /** The key of the key-value pair. */
    public K key;
    /** The value the key-value pair. */
    public V value;

    /** Creates a new key-value pair where all fields are null. */
    public KeyValue() {}

    private KeyValue(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(KeyValue<K, V> other) {
        int d = this.key.compareTo(other.key);
        if (d == 0) {
            return this.value.compareTo(other.value);
        }
        return d;
    }

    /** Creates a new key-value pair. */
    public static <K extends Comparable<? super K>, T1 extends Comparable<? super T1>>
            KeyValue<K, T1> of(K key, T1 value) {
        return new KeyValue<>(key, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KeyValue)) {
            return false;
        }
        @SuppressWarnings("rawtypes")
        KeyValue keyValue = (KeyValue) o;
        if (key != null ? !key.equals(keyValue.key) : keyValue.key != null) {
            return false;
        }
        if (value != null ? !value.equals(keyValue.value) : keyValue.value != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "("
                + StringUtils.arrayAwareToString(this.key)
                + ","
                + StringUtils.arrayAwareToString(this.value)
                + ")";
    }
}
