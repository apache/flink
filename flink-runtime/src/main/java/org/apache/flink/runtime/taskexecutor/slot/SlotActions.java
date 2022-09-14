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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.runtime.clusterframework.types.AllocationID;

import java.util.UUID;

/** Interface to trigger slot actions from within the {@link TaskSlotTable}. */
public interface SlotActions {

    /**
     * Free the task slot with the given allocation id.
     *
     * @param allocationId to identify the slot to be freed
     */
    void freeSlot(AllocationID allocationId);

    /**
     * Timeout the task slot for the given allocation id. The timeout is identified by the given
     * ticket to filter invalid timeouts out.
     *
     * @param allocationId identifying the task slot to be timed out
     * @param ticket allowing to filter invalid timeouts out
     */
    void timeoutSlot(AllocationID allocationId, UUID ticket);
}
