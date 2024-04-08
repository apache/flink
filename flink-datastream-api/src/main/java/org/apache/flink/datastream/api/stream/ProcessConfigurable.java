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

package org.apache.flink.datastream.api.stream;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.SlotSharingGroup;

/**
 * This represents the configuration handle of processing. Many processing-related properties can be
 * set through the `withYYY` method provided by this interface, such as withName, withUid, etc.
 */
@Experimental
public interface ProcessConfigurable<T extends ProcessConfigurable<T>> {
    /**
     * Sets an ID for this operator.
     *
     * <p>The specified ID is used to assign the same process operator ID across job submissions
     * (for example when starting a job from a savepoint).
     *
     * <p><strong>Important</strong>: this ID needs to be unique per transformation and job.
     * Otherwise, job submission will fail.
     *
     * @param uid The unique user-specified ID of this transformation.
     * @return The operator with the specified ID.
     */
    T withUid(String uid);

    /**
     * Sets the name of the current data stream. This name is used by the visualization and logging
     * during runtime.
     *
     * @return The named operator.
     */
    T withName(String name);

    /**
     * Sets the parallelism for this process operator.
     *
     * @param parallelism The parallelism for this operator.
     * @return The operator with set parallelism.
     */
    T withParallelism(int parallelism);

    /**
     * Sets the maximum parallelism of this operator.
     *
     * <p>The maximum parallelism specifies the upper bound for dynamic scaling. It also defines the
     * number of key groups used for partitioned state.
     *
     * @param maxParallelism Maximum parallelism
     * @return The operator with set maximum parallelism
     */
    T withMaxParallelism(int maxParallelism);

    /**
     * Sets the slot sharing group of this operation. Parallel instances of operations that are in
     * the same slot sharing group will be co-located in the same TaskManager slot, if possible.
     *
     * <p>Operations inherit the slot sharing group of input operations if all input operations are
     * in the same slot sharing group and no slot sharing group was explicitly specified.
     *
     * <p>Initially an operation is in the default slot sharing group. An operation can be put into
     * the default group explicitly by setting the slot sharing group with name {@code "default"}.
     *
     * @param slotSharingGroup Which contains name and its resource spec.
     */
    T withSlotSharingGroup(SlotSharingGroup slotSharingGroup);
}
