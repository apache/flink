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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.clusterframework.types.AllocationID;

import javax.annotation.Nullable;

/** A function for freeing slots. */
@FunctionalInterface
public interface FreeSlotFunction {
    /**
     * Frees the slot identified by the given {@link AllocationID}.
     *
     * <p>If the slot is freed due to exceptional circumstances a {@link Throwable} cause should be
     * provided.
     *
     * @param allocationId identifies the slot
     * @param cause reason for why the slot was freed; null during normal operations
     * @param timestamp when the slot was freed
     */
    void freeSlot(AllocationID allocationId, @Nullable Throwable cause, long timestamp);
}
