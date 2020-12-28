/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.util.collections;

import java.util.HashSet;

/** Wrap {@link HashSet} with hashSet interface. */
public class ObjectHashSet<T> extends OptimizableHashSet {

    private HashSet<T> set;

    public ObjectHashSet(final int expected, final float f) {
        super(expected, f);
        this.set = new HashSet<>(expected, f);
    }

    public ObjectHashSet(final int expected) {
        this(expected, DEFAULT_LOAD_FACTOR);
    }

    public ObjectHashSet() {
        this(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
    }

    public boolean add(T t) {
        return set.add(t);
    }

    public boolean contains(final T t) {
        return set.contains(t);
    }

    @Override
    public void optimize() {}
}
