package org.apache.flink.runtime.util;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.util.Preconditions;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class CircleIterator<K, V> implements Iterator<V> {
    private final Map<K, List<V>> values;
    private final List<K> orders;
    private int index;

    public CircleIterator(Map<K, List<V>> values, Comparator<K> comparator) {
        Preconditions.checkNotNull(values);
        Preconditions.checkNotNull(comparator);
        this.values = new HashMap<>(values);
        this.orders = values.keySet().stream().sorted(comparator).collect(Collectors.toList());
    }

    @Override
    public boolean hasNext() {
        for (List<V> list : values.values()) {
            if (!list.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public V next() {
        for (int i = 0; i < orders.size(); ++i) {
            List<V> candidate = nextCandidate();
            if (candidate.isEmpty()) {
                continue;
            }
            return candidate.remove(0);
        }
        throw new NoSuchElementException();
    }

    private List<V> nextCandidate() {
        int i = (index++) % orders.size();
        return values.get(orders.get(i));
    }
}
