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

package org.apache.flink.runtime.state.restore;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.StateMigrationException;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Modified {@link java.util.Iterator} interface that lets the implementation throw an {@link
 * IOException}.
 *
 * @param <E> the type of elements returned by this iterator
 * @see java.util.Iterator
 */
@Internal
public interface ThrowingIterator<E> extends Closeable {
    /**
     * Returns {@code true} if the iteration has more elements. (In other words, returns {@code
     * true} if {@link #next} would return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    boolean hasNext();

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    E next() throws IOException, StateMigrationException;
}
