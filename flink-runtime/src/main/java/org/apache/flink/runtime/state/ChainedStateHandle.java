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

package org.apache.flink.runtime.state;

import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;

/** Handle to state handles for the operators in an operator chain. */
public class ChainedStateHandle<T extends StateObject> implements StateObject {

    private static final long serialVersionUID = 1L;

    /** The state handles for all operators in the chain */
    private final List<? extends T> operatorStateHandles;

    /**
     * Wraps a list to the state handles for the operators in a chain. Individual state handles can
     * be null.
     *
     * @param operatorStateHandles list with the state handles for the states in the operator chain.
     */
    public ChainedStateHandle(List<? extends T> operatorStateHandles) {
        this.operatorStateHandles = Preconditions.checkNotNull(operatorStateHandles);
    }

    /**
     * Check if there are any states handles present. Notice that this can be true even if {@link
     * #getLength()} is greater than zero, because state handles can be null.
     *
     * @return true if there are no state handles for any operator.
     */
    public boolean isEmpty() {
        for (T state : operatorStateHandles) {
            if (state != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the length of the operator chain. This can be different from the number of operator
     * state handles, because the some operators in the chain can have no state and thus their state
     * handle can be null.
     *
     * @return length of the operator chain
     */
    public int getLength() {
        return operatorStateHandles.size();
    }

    /**
     * Get the state handle for a single operator in the operator chain by it's index.
     *
     * @param index the index in the operator chain
     * @return state handle to the operator at the given position in the operator chain. can be
     *     null.
     */
    public T get(int index) {
        return operatorStateHandles.get(index);
    }

    @Override
    public void discardState() throws Exception {
        StateUtil.bestEffortDiscardAllStateObjects(operatorStateHandles);
    }

    @Override
    public long getStateSize() {
        long sumStateSize = 0;

        if (operatorStateHandles != null) {
            for (T state : operatorStateHandles) {
                if (state != null) {
                    sumStateSize += state.getStateSize();
                }
            }
        }

        // State size as sum of all state sizes
        return sumStateSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ChainedStateHandle<?> that = (ChainedStateHandle<?>) o;

        return operatorStateHandles.equals(that.operatorStateHandles);
    }

    @Override
    public int hashCode() {
        return operatorStateHandles.hashCode();
    }

    public static <T extends StateObject> ChainedStateHandle<T> wrapSingleHandle(
            T stateHandleToWrap) {
        return new ChainedStateHandle<T>(Collections.singletonList(stateHandleToWrap));
    }

    public static boolean isNullOrEmpty(ChainedStateHandle<?> chainedStateHandle) {
        return chainedStateHandle == null || chainedStateHandle.isEmpty();
    }
}
