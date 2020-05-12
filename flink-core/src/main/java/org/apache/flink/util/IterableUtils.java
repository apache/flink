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

import java.util.Collection;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A collection of utilities that expand the usage of {@link Iterable}.
 */
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

		return iterable instanceof Collection ?
			((Collection<E>) iterable).stream() :
			StreamSupport.stream(iterable.spliterator(), false);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Private default constructor to avoid instantiation.
	 */
	private IterableUtils() {}
}
