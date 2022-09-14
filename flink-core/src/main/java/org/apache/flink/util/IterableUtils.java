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

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A collection of utilities that expand the usage of {@link Iterable}. */
public class IterableUtils {

    /**
     * Convert the given {@link Iterable} to a {@link Stream}.
     *
     * @param iterable to convert to a stream
     * @param <E> type of the elements of the iterable
     * @return stream converted from the given {@link Iterable}
     */
    public static <E> Stream<E> toStream(Iterable<E> iterable) {
        checkNotNull(iterable);

        return iterable instanceof Collection
                ? ((Collection<E>) iterable).stream()
                : StreamSupport.stream(iterable.spliterator(), false);
    }

    /**
     * Flatmap the two-dimensional {@link Iterable} into an one-dimensional {@link Iterable} and
     * convert the keys into items.
     *
     * @param itemGroups to flatmap
     * @param mapper convert the {@link K} into {@link V}
     * @param <K> type of key in the two-dimensional iterable
     * @param <V> type of items that are mapped to
     * @param <G> iterable of {@link K}
     * @return flattened one-dimensional {@link Iterable} from the given two-dimensional {@link
     *     Iterable}
     */
    @Internal
    public static <K, V, G extends Iterable<K>> Iterable<V> flatMap(
            Iterable<G> itemGroups, Function<K, V> mapper) {
        return () ->
                new Iterator<V>() {
                    private final Iterator<G> groupIterator = itemGroups.iterator();
                    private Iterator<K> itemIterator;

                    @Override
                    public boolean hasNext() {
                        while (itemIterator == null || !itemIterator.hasNext()) {
                            if (!groupIterator.hasNext()) {
                                return false;
                            } else {
                                itemIterator = groupIterator.next().iterator();
                            }
                        }
                        return true;
                    }

                    @Override
                    public V next() {
                        if (hasNext()) {
                            return mapper.apply(itemIterator.next());
                        } else {
                            throw new NoSuchElementException();
                        }
                    }
                };
    }

    // --------------------------------------------------------------------------------------------

    /** Private default constructor to avoid instantiation. */
    private IterableUtils() {}
}
