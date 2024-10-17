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

package org.apache.flink.runtime.state.v2.adaptor;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.state.StateFutureUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

/** A {@link org.apache.flink.api.common.state.v2.StateIterator} that has all elements. */
public class CompleteStateIterator<T> implements StateIterator<T> {

    final Iterator<T> iterator;
    final boolean empty;

    public CompleteStateIterator(Iterable<T> iterable) {
        this.iterator = iterable.iterator();
        this.empty = !iterator.hasNext();
    }

    @Override
    public <U> StateFuture<Collection<U>> onNext(Function<T, StateFuture<? extends U>> iterating) {
        if (isEmpty()) {
            return StateFutureUtils.completedFuture(Collections.emptyList());
        }
        Collection<StateFuture<? extends U>> resultFutures = new ArrayList<>();

        iterator.forEachRemaining(item -> resultFutures.add(iterating.apply(item)));
        return StateFutureUtils.combineAll(resultFutures);
    }

    @Override
    public StateFuture<Void> onNext(Consumer<T> iterating) {
        iterator.forEachRemaining(iterating);
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public boolean isEmpty() {
        return empty;
    }
}
