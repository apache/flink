/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import javax.annotation.Nonnull;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;

import static java.util.Arrays.asList;

/**
 * This interface represents an {@link Iterator} that is also {@link AutoCloseable}. A typical
 * use-case for this interface are iterators that are based on native-resources such as files,
 * network, or database connections. Clients must call {@link #close()} after using the iterator.
 *
 * @param <T> the type of iterated elements.
 */
public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

    CloseableIterator<?> EMPTY_INSTANCE =
            CloseableIterator.adapterForIterator(Collections.emptyIterator());

    @Nonnull
    static <T> CloseableIterator<T> adapterForIterator(@Nonnull Iterator<T> iterator) {
        return adapterForIterator(iterator, () -> {});
    }

    static <T> CloseableIterator<T> adapterForIterator(
            @Nonnull Iterator<T> iterator, AutoCloseable close) {
        return new IteratorAdapter<>(iterator, close);
    }

    static <T> CloseableIterator<T> fromList(List<T> list, Consumer<T> closeNotConsumed) {
        return new CloseableIterator<T>() {
            private final Deque<T> stack = new ArrayDeque<>(list);

            @Override
            public boolean hasNext() {
                return !stack.isEmpty();
            }

            @Override
            public T next() {
                return stack.poll();
            }

            @Override
            public void close() throws Exception {
                Exception exception = null;
                for (T el : stack) {
                    try {
                        closeNotConsumed.accept(el);
                    } catch (Exception e) {
                        exception = ExceptionUtils.firstOrSuppressed(e, exception);
                    }
                }
                if (exception != null) {
                    throw exception;
                }
            }
        };
    }

    static <T> CloseableIterator<T> flatten(CloseableIterator<T>... iterators) {
        return new CloseableIterator<T>() {
            private final Queue<CloseableIterator<T>> queue =
                    removeEmptyHead(new LinkedList<>(asList(iterators)));

            private Queue<CloseableIterator<T>> removeEmptyHead(Queue<CloseableIterator<T>> queue) {
                while (!queue.isEmpty() && !queue.peek().hasNext()) {
                    queue.poll();
                }
                return queue;
            }

            @Override
            public boolean hasNext() {
                removeEmptyHead(queue);
                return !queue.isEmpty();
            }

            @Override
            public T next() {
                removeEmptyHead(queue);
                return queue.peek().next();
            }

            @Override
            public void close() throws Exception {
                IOUtils.closeAll(iterators);
            }
        };
    }

    @SuppressWarnings("unchecked")
    static <T> CloseableIterator<T> empty() {
        return (CloseableIterator<T>) EMPTY_INSTANCE;
    }

    static <T> CloseableIterator<T> ofElements(Consumer<T> closeNotConsumed, T... elements) {
        return fromList(asList(elements), closeNotConsumed);
    }

    static <E> CloseableIterator<E> ofElement(E element, Consumer<E> closeIfNotConsumed) {
        return new CloseableIterator<E>() {
            private boolean hasNext = true;

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public E next() {
                hasNext = false;
                return element;
            }

            @Override
            public void close() {
                if (hasNext) {
                    closeIfNotConsumed.accept(element);
                }
            }
        };
    }

    /**
     * Adapter from {@link Iterator} to {@link CloseableIterator}. Does nothing on {@link #close()}.
     *
     * @param <E> the type of iterated elements.
     */
    final class IteratorAdapter<E> implements CloseableIterator<E> {

        @Nonnull private final Iterator<E> delegate;
        private final AutoCloseable close;

        IteratorAdapter(@Nonnull Iterator<E> delegate, AutoCloseable close) {
            this.delegate = delegate;
            this.close = close;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public E next() {
            return delegate.next();
        }

        @Override
        public void remove() {
            delegate.remove();
        }

        @Override
        public void forEachRemaining(Consumer<? super E> action) {
            delegate.forEachRemaining(action);
        }

        @Override
        public void close() throws Exception {
            close.close();
        }
    }
}
