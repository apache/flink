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

package org.apache.flink.state.changelog;

import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.BiConsumerWithException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;

class StateChangeLoggingIterator<State, StateElement, Namespace>
        implements CloseableIterator<StateElement> {

    private final CloseableIterator<StateElement> iterator;
    private final StateChangeLogger<State, Namespace> changeLogger;
    private final BiConsumerWithException<StateElement, DataOutputView, IOException> removalWriter;
    private final Namespace ns;
    @Nullable private StateElement lastReturned;

    private StateChangeLoggingIterator(
            CloseableIterator<StateElement> iterator,
            StateChangeLogger<State, Namespace> changeLogger,
            BiConsumerWithException<StateElement, DataOutputView, IOException> removalWriter,
            Namespace ns) {
        this.iterator = iterator;
        this.changeLogger = changeLogger;
        this.removalWriter = removalWriter;
        this.ns = ns;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public StateElement next() {
        return lastReturned = iterator.next();
    }

    @Override
    public void remove() {
        iterator.remove();
        try {
            changeLogger.valueElementRemoved(out -> removalWriter.accept(lastReturned, out), ns);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @Nonnull
    public static <Namespace, State, StateElement> CloseableIterator<StateElement> create(
            CloseableIterator<StateElement> iterator,
            StateChangeLogger<State, Namespace> changeLogger,
            BiConsumerWithException<StateElement, DataOutputView, IOException> removalWriter,
            Namespace ns) {
        return new StateChangeLoggingIterator<>(iterator, changeLogger, removalWriter, ns);
    }

    @Override
    public void close() throws Exception {
        iterator.close();
    }
}
