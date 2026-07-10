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

package org.apache.flink.state.table.filter;

import java.util.Collections;
import java.util.Set;

/** A filter that rejects every key. */
final class EmptyKeyFilterPlan<K> implements SavepointKeyFilterPlan<K> {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("rawtypes")
    private static final EmptyKeyFilterPlan INSTANCE = new EmptyKeyFilterPlan<>();

    private EmptyKeyFilterPlan() {}

    @SuppressWarnings("unchecked")
    static <K> EmptyKeyFilterPlan<K> instance() {
        return (EmptyKeyFilterPlan<K>) INSTANCE;
    }

    @Override
    public boolean test(K key) {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public Set<K> getExactKeys() {
        return Collections.emptySet();
    }

    @Override
    public SavepointKeyFilterPlan<K> intersect(SavepointKeyFilterPlan<K> other) {
        return this;
    }

    private Object readResolve() {
        return INSTANCE;
    }

    @Override
    public String toString() {
        return "EmptyKeyFilterPlan";
    }
}
