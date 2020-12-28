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

package org.apache.flink.runtime.persistence;

import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;
import java.io.Serializable;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link RetrievableStateStorageHelper} implementation for testing purposes. Set the functions or
 * handlers before {@link #store} if you want to do something else(e.g. throw an exception when the
 * state is specific value). Note: {@link TestingRetrievableStateHandle} is not serializable, so
 * this helper could only work with {@link TestingStateHandleStore}.
 *
 * @param <T> type of the element to store
 */
public final class TestingRetrievableStateStorageHelper<T extends Serializable>
        implements RetrievableStateStorageHelper<T> {

    private FunctionWithException<T, T, IOException> retrieveStateFunction = (state) -> state;

    private ThrowingConsumer<T, Exception> discardStateHandler = (state) -> {};

    private Function<T, Long> getStateSizeFunction = (state) -> 0L;

    @Override
    public RetrievableStateHandle<T> store(T state) {
        return new TestingRetrievableStateHandle(state);
    }

    public void setRetrieveStateFunction(
            FunctionWithException<T, T, IOException> retrieveStateFunction) {
        this.retrieveStateFunction = checkNotNull(retrieveStateFunction);
    }

    public void setDiscardStateHandler(ThrowingConsumer<T, Exception> discardStateHandler) {
        this.discardStateHandler = checkNotNull(discardStateHandler);
    }

    public void setGetStateSizeFunction(Function<T, Long> getStateSizeFunction) {
        this.getStateSizeFunction = checkNotNull(getStateSizeFunction);
    }

    private final class TestingRetrievableStateHandle implements RetrievableStateHandle<T> {

        private static final long serialVersionUID = 1L;

        private final T state;

        private TestingRetrievableStateHandle(T state) {
            this.state = state;
        }

        @Override
        public T retrieveState() throws IOException {
            return retrieveStateFunction.apply(state);
        }

        @Override
        public void discardState() throws Exception {
            discardStateHandler.accept(state);
        }

        @Override
        public long getStateSize() {
            return getStateSizeFunction.apply(state);
        }
    }
}
