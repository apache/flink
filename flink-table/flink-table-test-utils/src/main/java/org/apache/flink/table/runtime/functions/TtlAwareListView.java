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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.dataview.ListView;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * A {@link ListView} that tracks per-element write timestamps for TTL eviction.
 *
 * <p>Uses a {@link TimestampedList} as the backing list, which stores elements alongside their
 * timestamps in a single {@code ArrayList}. All mutations automatically record timestamps.
 */
@Internal
class TtlAwareListView<T> extends ListView<T> {

    private final LongSupplier clock;

    TtlAwareListView(LongSupplier clock) {
        this.clock = clock;
        super.setList(new TimestampedList<>(clock));
    }

    @Override
    public void setList(List<T> list) {
        TimestampedList<T> tsList = new TimestampedList<>(clock);
        tsList.addAll(list);
        super.setList(tsList);
    }

    void evictExpired(long currentTime, long ttlMillis) {
        asTsList().evictExpired(currentTime, ttlMillis);
    }

    List<Long> getTimestamps() {
        return asTsList().getTimestamps();
    }

    void setListWithTimestamps(List<T> list, List<Long> timestamps) {
        TimestampedList<T> tsList = new TimestampedList<>(clock);
        tsList.setDataAndTimestamps(list, timestamps);
        super.setList(tsList);
    }

    @SuppressWarnings("unchecked")
    private TimestampedList<T> asTsList() {
        return (TimestampedList<T>) getList();
    }

    /** List backed by timestamped values; mutations maintain a timestamp with each element. */
    private static class TimestampedList<E> extends AbstractList<E> {

        private final ArrayList<TimestampedValue<E>> list = new ArrayList<>();
        private final LongSupplier clock;

        TimestampedList(LongSupplier clock) {
            this.clock = clock;
        }

        @Override
        public E get(int index) {
            return list.get(index).value;
        }

        @Override
        public int size() {
            return list.size();
        }

        @Override
        public E set(int index, E element) {
            TimestampedValue<E> old =
                    list.set(index, new TimestampedValue<>(element, clock.getAsLong()));
            return old.value;
        }

        @Override
        public void add(int index, E element) {
            list.add(index, new TimestampedValue<>(element, clock.getAsLong()));
        }

        @Override
        public E remove(int index) {
            return list.remove(index).value;
        }

        @Override
        public void clear() {
            list.clear();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void sort(Comparator<? super E> c) {
            list.sort(
                    (a, b) -> {
                        if (c != null) {
                            return c.compare(a.value, b.value);
                        }
                        return ((Comparable<E>) a.value).compareTo(b.value);
                    });
        }

        void evictExpired(long currentTime, long ttlMillis) {
            list.removeIf(se -> currentTime - se.timestamp >= ttlMillis);
        }

        List<Long> getTimestamps() {
            return list.stream().map(se -> se.timestamp).collect(Collectors.toList());
        }

        void setDataAndTimestamps(List<? extends E> elements, List<Long> timestamps) {
            if (elements.size() != timestamps.size()) {
                throw new IllegalArgumentException(
                        "Elements and timestamps must have the same size: "
                                + elements.size()
                                + " vs "
                                + timestamps.size());
            }
            list.clear();
            for (int i = 0; i < elements.size(); i++) {
                list.add(new TimestampedValue<>(elements.get(i), timestamps.get(i)));
            }
        }
    }
}
