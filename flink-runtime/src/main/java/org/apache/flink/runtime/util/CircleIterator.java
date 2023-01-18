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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Iterator for merging multiple lists of varying sizes into a single list as uniformly as possible.
 *
 * @param <V> Element type
 */
public class CircleIterator<V> implements Iterator<V> {

    private final Random rnd = new Random();
    private final List<Group> groups;

    public CircleIterator(Collection<List<V>> lists) {
        Preconditions.checkNotNull(lists);
        double totalValues = lists.stream().mapToInt(List::size).sum();
        this.groups = new LinkedList<>();
        lists.stream()
                .filter(l -> !l.isEmpty())
                .forEach(l -> groups.add(new Group(new ArrayList<>(l), totalValues / l.size())));
        Collections.sort(groups, Comparator.comparingDouble(g -> g.total));
    }

    @Override
    public boolean hasNext() {
        return !groups.isEmpty();
    }

    @Override
    public V next() {
        // We get the next element from the group with the smallest total frequency
        Group nextGroup = groups.remove(0);
        V nextElement = nextGroup.removeNext();

        // Insert group back to the queue if it still has elements
        insertGroup(nextGroup);
        return nextElement;
    }

    /**
     * Insert group to the correct position by maintaining sort order on total frequency
     *
     * @param group Group to insert
     */
    private void insertGroup(Group group) {
        if (!group.elements.isEmpty()) {
            int insertIndex = groups.size();
            for (int i = 0; i < groups.size(); i++) {
                if (group.total < groups.get(i).total) {
                    insertIndex = i;
                    break;
                }
            }
            groups.add(insertIndex, group);
        }
    }

    public static <V> SortingFunction<V> sortFunction(Function<V, ?> groupingFunction) {
        return elements -> sort(elements, groupingFunction);
    }

    private static <V> List<V> sort(Collection<V> elements, Function<V, ?> groupingFunction) {
        Collection<List<V>> groups =
                elements.stream().collect(Collectors.groupingBy(groupingFunction)).values();

        List<V> out = new ArrayList<>(elements.size());
        new CircleIterator<>(groups).forEachRemaining(out::add);
        return out;
    }

    private class Group {
        private final List<V> elements;
        private final double frequency;
        private double total;

        Group(List<V> elements, double frequency) {
            this.elements = elements;
            this.frequency = frequency;

            // we initialize the total randomly between (0, frequency) to avoid clustering of
            // elements with the same initial frequency
            this.total = rnd.nextDouble() * frequency;
        }

        V removeNext() {
            V next = elements.remove(0);
            total += frequency;
            return next;
        }
    }
}
