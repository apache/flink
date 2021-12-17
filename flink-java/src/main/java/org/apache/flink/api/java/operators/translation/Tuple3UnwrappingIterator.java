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

package org.apache.flink.api.java.operators.translation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.TraversableOnceException;

import java.util.Iterator;

/**
 * An iterator that reads 3-tuples (groupKey, sortKey, value) and returns only the values (third
 * field). The iterator also tracks the groupKeys, as the triples flow though it.
 */
@Internal
public class Tuple3UnwrappingIterator<T, K1, K2>
        implements Iterator<T>, Iterable<T>, java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private K1 lastGroupKey;
    private K2 lastSortKey;
    private Iterator<Tuple3<K1, K2, T>> iterator;
    private boolean iteratorAvailable;

    public void set(Iterator<Tuple3<K1, K2, T>> iterator) {
        this.iterator = iterator;
        this.iteratorAvailable = true;
    }

    public K1 getLastGroupKey() {
        return lastGroupKey;
    }

    public K2 getLastSortKey() {
        return lastSortKey;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        Tuple3<K1, K2, T> t = iterator.next();
        this.lastGroupKey = t.f0;
        this.lastSortKey = t.f1;
        return t.f2;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
        if (iteratorAvailable) {
            iteratorAvailable = false;
            return this;
        } else {
            throw new TraversableOnceException();
        }
    }
}
