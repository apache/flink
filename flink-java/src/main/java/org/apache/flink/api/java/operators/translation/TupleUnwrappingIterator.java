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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.TraversableOnceException;

import java.util.Iterator;

/**
 * An iterator that reads 2-tuples (key value pairs) and returns only the values (second field). The
 * iterator also tracks the keys, as the pairs flow though it.
 */
@Internal
public class TupleUnwrappingIterator<T, K>
        implements Iterator<T>, Iterable<T>, java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private K lastKey;
    private Iterator<Tuple2<K, T>> iterator;
    private boolean iteratorAvailable;

    public void set(Iterator<Tuple2<K, T>> iterator) {
        this.iterator = iterator;
        this.iteratorAvailable = true;
    }

    public K getLastKey() {
        return lastKey;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        Tuple2<K, T> t = iterator.next();
        this.lastKey = t.f0;
        return t.f1;
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
