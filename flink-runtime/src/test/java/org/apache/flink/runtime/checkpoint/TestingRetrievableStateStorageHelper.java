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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.state.RetrievableStateHandle;

import java.io.Serializable;

/**
 * {@link RetrievableStateStorageHelper} implementation for testing purposes.
 *
 * @param <T> type of the element to store
 */
public final class TestingRetrievableStateStorageHelper<T extends Serializable>
        implements RetrievableStateStorageHelper<T> {

    @Override
    public RetrievableStateHandle<T> store(T state) {
        return new TestingRetrievableStateHandle<>(state);
    }

    private static final class TestingRetrievableStateHandle<T extends Serializable>
            implements RetrievableStateHandle<T> {

        private static final long serialVersionUID = 137053380713794300L;

        private final T state;

        private TestingRetrievableStateHandle(T state) {
            this.state = state;
        }

        @Override
        public T retrieveState() {
            return state;
        }

        @Override
        public void discardState() {
            // no op
        }

        @Override
        public long getStateSize() {
            return 0;
        }
    }
}
