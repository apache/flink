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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * This interface represents an iterable that is also closeable.
 *
 * @param <T> type of the iterated objects.
 */
public interface CloseableIterable<T> extends Iterable<T>, Closeable {

	/**
	 * Empty iterator.
	 */
	class Empty<T> implements CloseableIterable<T> {

		private Empty() {
		}

		@Override
		public void close() throws IOException {

		}

		@Nonnull
		@Override
		public Iterator<T> iterator() {
			return Collections.emptyIterator();
		}
	}

	/**
	 * Returns an empty iterator.
	 */
	static <T> CloseableIterable<T> empty() {
		return new CloseableIterable.Empty<>();
	}
}
