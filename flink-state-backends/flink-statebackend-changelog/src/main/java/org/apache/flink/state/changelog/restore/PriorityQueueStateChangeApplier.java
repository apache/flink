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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.state.changelog.StateChangeOperation;

class PriorityQueueStateChangeApplier<T> implements StateChangeApplier {
    private final KeyGroupedInternalPriorityQueue<T> queue;
    private final TypeSerializer<T> serializer;

    public PriorityQueueStateChangeApplier(
            KeyGroupedInternalPriorityQueue<T> queue, TypeSerializer<T> serializer) {
        this.queue = queue;
        this.serializer = serializer;
    }

    @Override
    public void apply(StateChangeOperation operation, DataInputView in) throws Exception {
        switch (operation) {
            case REMOVE_FIRST_ELEMENT:
                queue.poll();
                break;
            case ADD_ELEMENT:
                int numElements = in.readInt();
                for (int i = 0; i < numElements; i++) {
                    queue.add(serializer.deserialize(in));
                }
                break;
            case REMOVE_ELEMENT:
                queue.remove(serializer.deserialize(in));
                break;
            default:
                throw new UnsupportedOperationException(operation.name());
        }
    }
}
