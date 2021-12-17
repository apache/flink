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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;

import javax.annotation.Nonnull;

/** Factory for {@link KeyGroupedInternalPriorityQueue} instances. */
public interface PriorityQueueSetFactory {

    /**
     * Creates a {@link KeyGroupedInternalPriorityQueue}.
     *
     * @param stateName unique name for associated with this queue.
     * @param byteOrderedElementSerializer a serializer that with a format that is lexicographically
     *     ordered in alignment with elementPriorityComparator.
     * @param <T> type of the stored elements.
     * @return the queue with the specified unique name.
     */
    @Nonnull
    <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer);
}
