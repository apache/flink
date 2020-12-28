/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupPartitioner;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * This wrapper combines a HeapPriorityQueue with backend meta data.
 *
 * @param <T> type of the queue elements.
 */
public class HeapPriorityQueueSnapshotRestoreWrapper<T extends HeapPriorityQueueElement>
        implements StateSnapshotRestore {

    @Nonnull private final HeapPriorityQueueSet<T> priorityQueue;
    @Nonnull private final KeyExtractorFunction<T> keyExtractorFunction;
    @Nonnull private final RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo;
    @Nonnull private final KeyGroupRange localKeyGroupRange;
    @Nonnegative private final int totalKeyGroups;

    public HeapPriorityQueueSnapshotRestoreWrapper(
            @Nonnull HeapPriorityQueueSet<T> priorityQueue,
            @Nonnull RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo,
            @Nonnull KeyExtractorFunction<T> keyExtractorFunction,
            @Nonnull KeyGroupRange localKeyGroupRange,
            int totalKeyGroups) {

        this.priorityQueue = priorityQueue;
        this.keyExtractorFunction = keyExtractorFunction;
        this.metaInfo = metaInfo;
        this.localKeyGroupRange = localKeyGroupRange;
        this.totalKeyGroups = totalKeyGroups;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public StateSnapshot stateSnapshot() {
        final T[] queueDump =
                (T[]) priorityQueue.toArray(new HeapPriorityQueueElement[priorityQueue.size()]);
        return new HeapPriorityQueueStateSnapshot<>(
                queueDump,
                keyExtractorFunction,
                metaInfo.deepCopy(),
                localKeyGroupRange,
                totalKeyGroups);
    }

    @Nonnull
    @Override
    public StateSnapshotKeyGroupReader keyGroupReader(int readVersionHint) {
        final TypeSerializer<T> elementSerializer = metaInfo.getElementSerializer();
        return KeyGroupPartitioner.createKeyGroupPartitionReader(
                elementSerializer
                        ::deserialize, // we know that this does not deliver nulls, because we never
                // write nulls
                (element, keyGroupId) -> priorityQueue.add(element));
    }

    @Nonnull
    public HeapPriorityQueueSet<T> getPriorityQueue() {
        return priorityQueue;
    }

    @Nonnull
    public RegisteredPriorityQueueStateBackendMetaInfo<T> getMetaInfo() {
        return metaInfo;
    }

    /**
     * Returns a deep copy of the snapshot, where the serializer is changed to the given serializer.
     */
    public HeapPriorityQueueSnapshotRestoreWrapper<T> forUpdatedSerializer(
            @Nonnull TypeSerializer<T> updatedSerializer) {

        RegisteredPriorityQueueStateBackendMetaInfo<T> updatedMetaInfo =
                new RegisteredPriorityQueueStateBackendMetaInfo<>(
                        metaInfo.getName(), updatedSerializer);

        return new HeapPriorityQueueSnapshotRestoreWrapper<>(
                priorityQueue,
                updatedMetaInfo,
                keyExtractorFunction,
                localKeyGroupRange,
                totalKeyGroups);
    }
}
