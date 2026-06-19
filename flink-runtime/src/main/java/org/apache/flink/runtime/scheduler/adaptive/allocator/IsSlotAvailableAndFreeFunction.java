/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

/** Functional interface for checking whether a slot is available and free. */
@FunctionalInterface
public interface IsSlotAvailableAndFreeFunction {

    /**
     * Returns {@code true} if a slot with the given {@link AllocationID} is available and free.
     *
     * @param allocationId allocationId specifies the slot to check
     * @return {@code true} if a slot with the given allocationId is available and free; otherwise
     *     {@code false}
     */
    boolean isSlotAvailableAndFree(AllocationID allocationId);
}
