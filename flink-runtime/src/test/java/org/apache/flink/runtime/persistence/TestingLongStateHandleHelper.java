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
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.AbstractID;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * Testing implementation for {@link RetrievableStateStorageHelper} and {@link
 * RetrievableStateHandle} with type {@link Long}.
 */
public class TestingLongStateHandleHelper
        implements RetrievableStateStorageHelper<TestingLongStateHandleHelper.LongStateHandle> {

    private static final List<LongStateHandle> STATE_STORAGE = new ArrayList<>();

    @Override
    public RetrievableStateHandle<LongStateHandle> store(LongStateHandle state) {
        final int pos = STATE_STORAGE.size();
        STATE_STORAGE.add(state);

        return new LongRetrievableStateHandle(pos);
    }

    public static LongStateHandle createState(long value) {
        return new LongStateHandle(value);
    }

    public static long getStateHandleValueByIndex(int index) {
        return STATE_STORAGE.get(index).getValue();
    }

    public static int getDiscardCallCountForStateHandleByIndex(int index) {
        return STATE_STORAGE.get(index).getNumberOfDiscardCalls();
    }

    public static int getGlobalStorageSize() {
        return STATE_STORAGE.size();
    }

    public static void clearGlobalState() {
        STATE_STORAGE.clear();
    }

    public static int getGlobalDiscardCount() {
        return STATE_STORAGE.stream().mapToInt(LongStateHandle::getNumberOfDiscardCalls).sum();
    }

    /**
     * {@code LongStateHandle} implements {@link StateObject} to monitor the {@link
     * StateObject#discardState()} calls.
     */
    public static class LongStateHandle implements StateObject {

        private static final long serialVersionUID = -5752042587113549855L;

        private final Long value;

        private int numberOfDiscardCalls = 0;

        public LongStateHandle(long value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        @Override
        public void discardState() {
            numberOfDiscardCalls++;
        }

        public int getNumberOfDiscardCalls() {
            return numberOfDiscardCalls;
        }

        @Override
        public long getStateSize() {
            return 8L;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", LongStateHandle.class.getSimpleName() + "[", "]")
                    .add("value=" + value)
                    .add("numberOfDiscardCalls=" + numberOfDiscardCalls)
                    .toString();
        }
    }

    /** Testing {@link RetrievableStateStorageHelper} implementation with {@link Long}. */
    public static class LongRetrievableStateHandle
            implements RetrievableStateHandle<LongStateHandle> {

        private static final long serialVersionUID = -3555329254423838912L;

        private final int stateReference;

        public LongRetrievableStateHandle(int stateReference) {
            this.stateReference = stateReference;
        }

        @Override
        public LongStateHandle retrieveState() {
            return STATE_STORAGE.get(stateReference);
        }

        @Override
        public void discardState() {
            STATE_STORAGE.get(stateReference).discardState();
        }

        @Override
        public long getStateSize() {
            return AbstractID.SIZE;
        }
    }
}
