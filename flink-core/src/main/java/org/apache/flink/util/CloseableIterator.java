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

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * This interface represents an {@link Iterator} that is also {@link AutoCloseable}. A typical use-case for this
 * interface are iterators that are based on native-resources such as files, network, or database connections. Clients
 * must call {@link #close()} after using the iterator.
 *
 * @param <T> the type of iterated elements.
 */
public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

	CloseableIterator<?> EMPTY_INSTANCE = CloseableIterator.adapterForIterator(Collections.emptyIterator());

	@Nonnull
	static <T> CloseableIterator<T> adapterForIterator(@Nonnull Iterator<T> iterator) {
		return new IteratorAdapter<>(iterator);
	}

	@SuppressWarnings("unchecked")
	static <T> CloseableIterator<T> empty() {
		return (CloseableIterator<T>) EMPTY_INSTANCE;
	}

	/**
	 * Adapter from {@link Iterator} to {@link CloseableIterator}. Does nothing on {@link #close()}.
	 *
	 * @param <E> the type of iterated elements.
	 */
	final class IteratorAdapter<E> implements CloseableIterator<E> {

		@Nonnull
		private final Iterator<E> delegate;

		IteratorAdapter(@Nonnull Iterator<E> delegate) {
			this.delegate = delegate;
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
		public void close() {
		}
	}
}
