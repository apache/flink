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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashSet;

public class TaskManagerRegistration {

    private final TaskExecutorConnection taskManagerConnection;

    private final HashSet<SlotID> slots;

    private int numberFreeSlots;

    /** Timestamp when the last time becoming idle. Otherwise Long.MAX_VALUE. */
    private long idleSince;

    public TaskManagerRegistration(
            TaskExecutorConnection taskManagerConnection, Collection<SlotID> slots) {

        this.taskManagerConnection =
                Preconditions.checkNotNull(taskManagerConnection, "taskManagerConnection");
        Preconditions.checkNotNull(slots, "slots");

        this.slots = new HashSet<>(slots);

        this.numberFreeSlots = slots.size();

        idleSince = System.currentTimeMillis();
    }

    public TaskExecutorConnection getTaskManagerConnection() {
        return taskManagerConnection;
    }

    public InstanceID getInstanceId() {
        return taskManagerConnection.getInstanceID();
    }

    public int getNumberRegisteredSlots() {
        return slots.size();
    }

    public int getNumberFreeSlots() {
        return numberFreeSlots;
    }

    public void freeSlot() {
        Preconditions.checkState(
                numberFreeSlots < slots.size(),
                "The number of free slots cannot exceed the number of registered slots. This indicates a bug.");
        numberFreeSlots++;

        if (numberFreeSlots == getNumberRegisteredSlots() && idleSince == Long.MAX_VALUE) {
            idleSince = System.currentTimeMillis();
        }
    }

    public void occupySlot() {
        Preconditions.checkState(
                numberFreeSlots > 0, "There are no more free slots. This indicates a bug.");
        numberFreeSlots--;

        idleSince = Long.MAX_VALUE;
    }

    public Iterable<SlotID> getSlots() {
        return slots;
    }

    public long getIdleSince() {
        return idleSince;
    }

    public boolean isIdle() {
        return idleSince != Long.MAX_VALUE;
    }

    public void markUsed() {
        idleSince = Long.MAX_VALUE;
    }

    public boolean containsSlot(SlotID slotId) {
        return slots.contains(slotId);
    }
}
