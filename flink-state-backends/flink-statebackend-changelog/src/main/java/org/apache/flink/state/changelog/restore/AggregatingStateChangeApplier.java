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

package org.apache.flink.state.changelog.restore;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.state.changelog.StateChangeOperation;

class AggregatingStateChangeApplier<K, N, IN, SV, OUT> extends KvStateChangeApplier<K, N> {
    private final InternalAggregatingState<K, N, IN, SV, OUT> state;

    protected AggregatingStateChangeApplier(
            InternalKeyContext<K> keyContext, InternalAggregatingState<K, N, IN, SV, OUT> state) {
        super(keyContext);
        this.state = state;
    }

    @Override
    protected InternalKvState<K, N, ?> getState() {
        return state;
    }

    protected void applyInternal(StateChangeOperation operation, DataInputView in)
            throws Exception {
        switch (operation) {
            case SET:
            case SET_INTERNAL:
                state.updateInternal(state.getValueSerializer().deserialize(in));
                break;
            case MERGE_NS:
                applyMergeNamespaces(state, in);
                break;
            case CLEAR:
                state.clear();
                break;
            default:
                throw new IllegalArgumentException("Unknown state change operation: " + operation);
        }
    }
}
