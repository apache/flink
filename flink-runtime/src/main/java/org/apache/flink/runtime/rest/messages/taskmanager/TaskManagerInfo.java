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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.ResourceIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.ResourceIDSerializer;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;
import java.util.Objects;

/**
 * Base class containing information for a {@link TaskExecutor}.
 */
public class TaskManagerInfo implements ResponseBody, Serializable {

	public static final String FIELD_NAME_RESOURCE_ID = "id";

	public static final String FIELD_NAME_ADDRESS = "path";

	public static final String FIELD_NAME_DATA_PORT = "dataPort";

	public static final String FIELD_NAME_LAST_HEARTBEAT = "timeSinceLastHeartbeat";

	public static final String FIELD_NAME_NUMBER_SLOTS = "slotsNumber";

	public static final String FIELD_NAME_NUMBER_AVAILABLE_SLOTS = "freeSlots";

	public static final String FIELD_NAME_HARDWARE = "hardware";

	private static final long serialVersionUID = 1L;

	@JsonProperty(FIELD_NAME_RESOURCE_ID)
	@JsonSerialize(using = ResourceIDSerializer.class)
	private final ResourceID resourceId;

	@JsonProperty(FIELD_NAME_ADDRESS)
	private final String address;

	@JsonProperty(FIELD_NAME_DATA_PORT)
	private final int dataPort;

	@JsonProperty(FIELD_NAME_LAST_HEARTBEAT)
	private final long lastHeartbeat;

	@JsonProperty(FIELD_NAME_NUMBER_SLOTS)
	private final int numberSlots;

	@JsonProperty(FIELD_NAME_NUMBER_AVAILABLE_SLOTS)
	private final int numberAvailableSlots;

	@JsonProperty(FIELD_NAME_HARDWARE)
	private final HardwareDescription hardwareDescription;

	@JsonCreator
	public TaskManagerInfo(
			@JsonDeserialize(using = ResourceIDDeserializer.class) @JsonProperty(FIELD_NAME_RESOURCE_ID) ResourceID resourceId,
			@JsonProperty(FIELD_NAME_ADDRESS) String address,
			@JsonProperty(FIELD_NAME_DATA_PORT) int dataPort,
			@JsonProperty(FIELD_NAME_LAST_HEARTBEAT) long lastHeartbeat,
			@JsonProperty(FIELD_NAME_NUMBER_SLOTS) int numberSlots,
			@JsonProperty(FIELD_NAME_NUMBER_AVAILABLE_SLOTS) int numberAvailableSlots,
			@JsonProperty(FIELD_NAME_HARDWARE) HardwareDescription hardwareDescription) {
		this.resourceId = Preconditions.checkNotNull(resourceId);
		this.address = Preconditions.checkNotNull(address);
		this.dataPort = dataPort;
		this.lastHeartbeat = lastHeartbeat;
		this.numberSlots = numberSlots;
		this.numberAvailableSlots = numberAvailableSlots;
		this.hardwareDescription = Preconditions.checkNotNull(hardwareDescription);
	}

	public ResourceID getResourceId() {
		return resourceId;
	}

	public String getAddress() {
		return address;
	}

	public int getDataPort() {
		return dataPort;
	}

	public long getLastHeartbeat() {
		return lastHeartbeat;
	}

	public int getNumberSlots() {
		return numberSlots;
	}

	public int getNumberAvailableSlots() {
		return numberAvailableSlots;
	}

	public HardwareDescription getHardwareDescription() {
		return hardwareDescription;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TaskManagerInfo that = (TaskManagerInfo) o;
		return dataPort == that.dataPort &&
			lastHeartbeat == that.lastHeartbeat &&
			numberSlots == that.numberSlots &&
			numberAvailableSlots == that.numberAvailableSlots &&
			Objects.equals(resourceId, that.resourceId) &&
			Objects.equals(address, that.address) &&
			Objects.equals(hardwareDescription, that.hardwareDescription);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			resourceId,
			address,
			dataPort,
			lastHeartbeat,
			numberSlots,
			numberAvailableSlots,
			hardwareDescription);
	}
}
