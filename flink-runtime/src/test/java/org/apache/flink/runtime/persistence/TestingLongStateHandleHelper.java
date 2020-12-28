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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Testing implementation for {@link RetrievableStateStorageHelper} and {@link
 * RetrievableStateHandle} with type {@link Long}.
 */
public class TestingLongStateHandleHelper implements RetrievableStateStorageHelper<Long> {

    private final List<LongRetrievableStateHandle> stateHandles = new ArrayList<>();

    @Override
    public RetrievableStateHandle<Long> store(Long state) {
        final LongRetrievableStateHandle stateHandle = new LongRetrievableStateHandle(state);
        stateHandles.add(stateHandle);

        return stateHandle;
    }

    public List<LongRetrievableStateHandle> getStateHandles() {
        return stateHandles;
    }

    /** Testing {@link RetrievableStateStorageHelper} implementation with {@link Long}. */
    public static class LongRetrievableStateHandle implements RetrievableStateHandle<Long> {

        private static final long serialVersionUID = -3555329254423838912L;

        private static AtomicInteger numberOfGlobalDiscardCalls = new AtomicInteger(0);

        private final Long state;

        private int numberOfDiscardCalls = 0;

        public LongRetrievableStateHandle(Long state) {
            this.state = state;
        }

        @Override
        public Long retrieveState() {
            return state;
        }

        @Override
        public void discardState() {
            numberOfGlobalDiscardCalls.incrementAndGet();
            numberOfDiscardCalls++;
        }

        @Override
        public long getStateSize() {
            return 0;
        }

        public int getNumberOfDiscardCalls() {
            return numberOfDiscardCalls;
        }

        public static int getNumberOfGlobalDiscardCalls() {
            return numberOfGlobalDiscardCalls.get();
        }

        public static void clearNumberOfGlobalDiscardCalls() {
            numberOfGlobalDiscardCalls.set(0);
        }
    }
}
