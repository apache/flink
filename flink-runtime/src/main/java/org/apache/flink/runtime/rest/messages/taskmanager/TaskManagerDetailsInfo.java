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

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.resourcemanager.TaskManagerInfoWithSlots;
import org.apache.flink.runtime.rest.messages.ResourceProfileInfo;
import org.apache.flink.runtime.rest.messages.json.ResourceIDDeserializer;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Collection;
import java.util.Objects;

/** Message containing base information about a {@link TaskExecutor} and more detailed metrics. */
public class TaskManagerDetailsInfo extends TaskManagerInfo {

    public static final String FIELD_NAME_METRICS = "metrics";

    public static final String FIELD_NAME_ALLOCATED_SLOTS = "allocatedSlots";

    @JsonProperty(FIELD_NAME_METRICS)
    private final TaskManagerMetricsInfo taskManagerMetrics;

    @JsonProperty(FIELD_NAME_ALLOCATED_SLOTS)
    private final Collection<SlotInfo> allocatedSlots;

    @JsonCreator
    public TaskManagerDetailsInfo(
            @JsonDeserialize(using = ResourceIDDeserializer.class)
                    @JsonProperty(FIELD_NAME_RESOURCE_ID)
                    ResourceID resourceId,
            @JsonProperty(FIELD_NAME_ADDRESS) String address,
            @JsonProperty(FIELD_NAME_DATA_PORT) int dataPort,
            @JsonProperty(FIELD_NAME_JMX_PORT) int jmxPort,
            @JsonProperty(FIELD_NAME_LAST_HEARTBEAT) long lastHeartbeat,
            @JsonProperty(FIELD_NAME_NUMBER_SLOTS) int numberSlots,
            @JsonProperty(FIELD_NAME_NUMBER_AVAILABLE_SLOTS) int numberAvailableSlots,
            @JsonProperty(FIELD_NAME_TOTAL_RESOURCE) ResourceProfileInfo totalResource,
            @JsonProperty(FIELD_NAME_AVAILABLE_RESOURCE) ResourceProfileInfo freeResource,
            @JsonProperty(FIELD_NAME_HARDWARE) HardwareDescription hardwareDescription,
            @JsonProperty(FIELD_NAME_MEMORY) TaskExecutorMemoryConfiguration memoryConfiguration,
            @JsonProperty(FIELD_NAME_ALLOCATED_SLOTS) Collection<SlotInfo> allocatedSlots,
            @JsonProperty(FIELD_NAME_METRICS) TaskManagerMetricsInfo taskManagerMetrics) {
        super(
                resourceId,
                address,
                dataPort,
                jmxPort,
                lastHeartbeat,
                numberSlots,
                numberAvailableSlots,
                totalResource,
                freeResource,
                hardwareDescription,
                memoryConfiguration);

        this.taskManagerMetrics = Preconditions.checkNotNull(taskManagerMetrics);
        this.allocatedSlots = Preconditions.checkNotNull(allocatedSlots);
    }

    public TaskManagerDetailsInfo(
            TaskManagerInfoWithSlots taskManagerInfoWithSlots,
            TaskManagerMetricsInfo taskManagerMetrics) {
        this(
                taskManagerInfoWithSlots.getTaskManagerInfo().getResourceId(),
                taskManagerInfoWithSlots.getTaskManagerInfo().getAddress(),
                taskManagerInfoWithSlots.getTaskManagerInfo().getDataPort(),
                taskManagerInfoWithSlots.getTaskManagerInfo().getJmxPort(),
                taskManagerInfoWithSlots.getTaskManagerInfo().getLastHeartbeat(),
                taskManagerInfoWithSlots.getTaskManagerInfo().getNumberSlots(),
                taskManagerInfoWithSlots.getTaskManagerInfo().getNumberAvailableSlots(),
                taskManagerInfoWithSlots.getTaskManagerInfo().getTotalResource(),
                taskManagerInfoWithSlots.getTaskManagerInfo().getFreeResource(),
                taskManagerInfoWithSlots.getTaskManagerInfo().getHardwareDescription(),
                taskManagerInfoWithSlots.getTaskManagerInfo().getMemoryConfiguration(),
                taskManagerInfoWithSlots.getAllocatedSlots(),
                taskManagerMetrics);
    }

    @JsonIgnore
    @VisibleForTesting
    public final TaskManagerMetricsInfo getTaskManagerMetricsInfo() {
        return this.taskManagerMetrics;
    }

    @JsonIgnore
    public Collection<SlotInfo> getAllocatedSlots() {
        return allocatedSlots;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TaskManagerDetailsInfo that = (TaskManagerDetailsInfo) o;
        return Objects.equals(taskManagerMetrics, that.taskManagerMetrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), taskManagerMetrics);
    }
}
