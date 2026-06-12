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

package org.apache.flink.state.api.input;

import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.util.NoSuchElementException;
import java.util.function.Predicate;

/** A {@link CloseableIterator} that only exposes elements matching a predicate. */
final class FilteringCloseableIterator<T> implements CloseableIterator<T> {

    private final CloseableIterator<T> source;
    private final Predicate<T> predicate;

    @Nullable private T pending;
    private boolean hasPending;

    FilteringCloseableIterator(CloseableIterator<T> source, Predicate<T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    public boolean hasNext() {
        while (!hasPending && source.hasNext()) {
            final T candidate = source.next();
            if (predicate.test(candidate)) {
                pending = candidate;
                hasPending = true;
            }
        }

        return hasPending;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        final T result = pending;
        pending = null;
        hasPending = false;
        return result;
    }

    @Override
    public void close() throws Exception {
        source.close();
    }
}
