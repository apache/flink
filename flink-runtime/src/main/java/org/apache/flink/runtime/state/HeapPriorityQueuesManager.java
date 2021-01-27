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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StateMigrationException;

import javax.annotation.Nonnull;

import java.util.Map;

/** Manages creating heap priority queues along with their counterpart meta info. */
@Internal
public class HeapPriorityQueuesManager {

    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final HeapPriorityQueueSetFactory priorityQueueSetFactory;
    private final KeyGroupRange keyGroupRange;
    private final int numberOfKeyGroups;

    public HeapPriorityQueuesManager(
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            KeyGroupRange keyGroupRange,
            int numberOfKeyGroups) {
        this.registeredPQStates = registeredPQStates;
        this.priorityQueueSetFactory = priorityQueueSetFactory;
        this.keyGroupRange = keyGroupRange;
        this.numberOfKeyGroups = numberOfKeyGroups;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> createOrUpdate(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {

        final HeapPriorityQueueSnapshotRestoreWrapper<T> existingState =
                (HeapPriorityQueueSnapshotRestoreWrapper<T>) registeredPQStates.get(stateName);

        if (existingState != null) {
            TypeSerializerSchemaCompatibility<T> compatibilityResult =
                    existingState
                            .getMetaInfo()
                            .updateElementSerializer(byteOrderedElementSerializer);

            if (compatibilityResult.isIncompatible()) {
                throw new FlinkRuntimeException(
                        new StateMigrationException(
                                "For heap backends, the new priority queue serializer must not be incompatible."));
            } else {
                registeredPQStates.put(
                        stateName,
                        existingState.forUpdatedSerializer(byteOrderedElementSerializer));
            }

            return existingState.getPriorityQueue();
        } else {
            final RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo =
                    new RegisteredPriorityQueueStateBackendMetaInfo<>(
                            stateName, byteOrderedElementSerializer);
            return createInternal(metaInfo);
        }
    }

    @Nonnull
    private <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> createInternal(
                    RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo) {

        final String stateName = metaInfo.getName();
        final HeapPriorityQueueSet<T> priorityQueue =
                priorityQueueSetFactory.create(stateName, metaInfo.getElementSerializer());

        HeapPriorityQueueSnapshotRestoreWrapper<T> wrapper =
                new HeapPriorityQueueSnapshotRestoreWrapper<>(
                        priorityQueue,
                        metaInfo,
                        KeyExtractorFunction.forKeyedObjects(),
                        keyGroupRange,
                        numberOfKeyGroups);

        registeredPQStates.put(stateName, wrapper);
        return priorityQueue;
    }

    public Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> getRegisteredPQStates() {
        return registeredPQStates;
    }
}
