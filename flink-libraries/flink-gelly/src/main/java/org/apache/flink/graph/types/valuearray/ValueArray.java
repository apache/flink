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

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.NormalizableKey;
import org.apache.flink.types.ResettableValue;
import org.apache.flink.types.Value;

import java.io.Serializable;

/**
 * Basic interface for array types which reuse objects during serialization.
 *
 * <p>Value arrays are usable as grouping keys but not sorting keys.
 *
 * @param <T> the {@link Value} type
 */
@TypeInfo(ValueArrayTypeInfoFactory.class)
public interface ValueArray<T>
        extends Iterable<T>,
                IOReadableWritable,
                Serializable,
                NormalizableKey<ValueArray<T>>,
                ResettableValue<ValueArray<T>>,
                CopyableValue<ValueArray<T>> {

    /**
     * Returns the number of elements stored in the array.
     *
     * @return the number of elements stored in the array
     */
    int size();

    /**
     * An bounded array fills when the allocated capacity has been fully used. An unbounded array
     * will only fill when the underlying data structure has reached capacity, for example the ~2^31
     * element limit for Java arrays.
     *
     * @return whether the array is full
     */
    boolean isFull();

    /**
     * Appends the value to this array if and only if the array capacity would not be exceeded.
     *
     * @param value the value to add to this array
     * @return whether the value was added to the array
     */
    boolean add(T value);

    /**
     * Appends all of the values in the specified array to the end of this array. If the combined
     * array would exceed capacity then no values are appended.
     *
     * @param source array containing values to be added to this array
     * @return whether the values were added to the array
     */
    boolean addAll(ValueArray<T> source);

    /**
     * Saves the array index, which can be restored by calling {@code reset()}.
     *
     * <p>This is not serialized and is not part of the contract for {@link #equals(Object)}.
     */
    void mark();

    /** Restores the array index to when {@code mark()} was last called. */
    void reset();

    /**
     * Resets the array to the empty state. The implementation is *not* expected to release the
     * underlying data structure. This allows the array to be reused with minimal impact on the
     * garbage collector.
     *
     * <p>This may reset the {@link #mark()} in order to allow arrays be shrunk.
     */
    void clear();
}
