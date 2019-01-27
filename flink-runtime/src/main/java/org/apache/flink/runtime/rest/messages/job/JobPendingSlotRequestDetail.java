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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.rest.messages.json.AbstractIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.AbstractIDSerializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDSerializer;
import org.apache.flink.runtime.rest.messages.json.SlotRequestIdDeserializer;
import org.apache.flink.runtime.rest.messages.json.SlotRequestIdSerializer;
import org.apache.flink.runtime.rest.messages.json.SlotSharingGroupIdDeserializer;
import org.apache.flink.runtime.rest.messages.json.SlotSharingGroupIdSerializer;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Base class containing information for a pending slot request of a job.
 */
public class JobPendingSlotRequestDetail implements Serializable {

	private static final long serialVersionUID = -7278419733850920818L;

	public static final String FIELD_NAME_SLOT_REQUEST_ID = "id";
	public static final String FIELD_NAME_RESOURCE_PROFILE = "resource_profile";
	public static final String FIELD_NAME_START_TIME = "start_time";
	public static final String FIELD_NAME_SLOT_SHARING_GROUP_ID = "sharing_id";
	public static final String FIELD_NAME_COLOCATION_GROUP_ID = "co-location_id";
	public static final String FIELD_NAME_JOB_VERTEX_TASKS = "tasks";

	public static final String FIELD_NAME_RESOURCE_CPU_CORES = "cpu_cores";
	public static final String FIELD_NAME_RESOURCE_HEAP_MEMORY = "heap_memory";
	public static final String FIELD_NAME_RESOURCE_DIRECT_MEMORY = "direct_memory";
	public static final String FIELD_NAME_RESOURCE_NATIVE_MEMORY = "native_memory";
	public static final String FIELD_NAME_RESOURCE_NETWORK_MEMORY = "network_memory";
	public static final String FIELD_NAME_RESOURCE_MANAGED_MEMORY = "managed_memory";

	@JsonProperty(FIELD_NAME_SLOT_REQUEST_ID)
	@JsonSerialize(using = SlotRequestIdSerializer.class)
	private final SlotRequestId slotRequestId;

	@JsonProperty(FIELD_NAME_RESOURCE_PROFILE)
	@JsonSerialize(using = JobPendingSlotRequestDetail.ResourceProfileSerializer.class)
	private final ResourceProfile resourceProfile;

	@JsonProperty(FIELD_NAME_START_TIME)
	private final long startTime;

	@JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_ID)
	@JsonSerialize(using = SlotSharingGroupIdSerializer.class)
	private final SlotSharingGroupId slotSharingGroupId;

	@JsonProperty(FIELD_NAME_COLOCATION_GROUP_ID)
	@JsonSerialize(using = AbstractIDSerializer.class)
	private final AbstractID coLocationGroupId;

	@JsonProperty(FIELD_NAME_JOB_VERTEX_TASKS)
	private final Collection<VertexTaskInfo> vertexTaskInfos;

	@JsonCreator
	public JobPendingSlotRequestDetail(
			@JsonProperty(FIELD_NAME_SLOT_REQUEST_ID) @JsonDeserialize(using = SlotRequestIdDeserializer.class)
					SlotRequestId slotRequestId,
			@JsonProperty(FIELD_NAME_RESOURCE_PROFILE) @JsonDeserialize(using = JobPendingSlotRequestDetail.ResourceProfileDeserializer.class)
					ResourceProfile resourceProfile,
			@JsonProperty(FIELD_NAME_START_TIME) long startTime,
			@JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_ID) @JsonDeserialize(using = SlotSharingGroupIdDeserializer.class) @Nullable
					SlotSharingGroupId slotSharingGroupId,
			@JsonProperty(FIELD_NAME_COLOCATION_GROUP_ID) @JsonDeserialize(using = AbstractIDDeserializer.class) @Nullable
					AbstractID coLocationGroupId,
			@JsonProperty(FIELD_NAME_JOB_VERTEX_TASKS) Collection<VertexTaskInfo> vertexTaskInfos) {
		this.slotRequestId = Preconditions.checkNotNull(slotRequestId);
		this.resourceProfile = Preconditions.checkNotNull(resourceProfile);
		this.startTime = startTime;
		this.slotSharingGroupId = slotSharingGroupId;
		this.coLocationGroupId = coLocationGroupId;
		this.vertexTaskInfos = Preconditions.checkNotNull(vertexTaskInfos);
	}

	public SlotRequestId getSlotRequestId() {
		return slotRequestId;
	}

	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public long getStartTime() {
		return startTime;
	}

	public SlotSharingGroupId getSlotSharingGroupId() {
		return slotSharingGroupId;
	}

	public AbstractID getCoLocationGroupId() {
		return coLocationGroupId;
	}

	public Collection<VertexTaskInfo> getVertexTaskInfos() {
		return vertexTaskInfos;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		JobPendingSlotRequestDetail that = (JobPendingSlotRequestDetail) o;
		return Objects.equals(slotRequestId, that.slotRequestId) &&
				Objects.equals(resourceProfile, that.resourceProfile) &&
				startTime == that.startTime &&
				Objects.equals(slotSharingGroupId, that.slotSharingGroupId) &&
				Objects.equals(coLocationGroupId, that.coLocationGroupId) &&
				Objects.equals(vertexTaskInfos, that.vertexTaskInfos);
	}

	@Override
	public int hashCode() {
		return Objects.hash(slotRequestId, resourceProfile, startTime, slotSharingGroupId, coLocationGroupId, vertexTaskInfos);
	}

	//---------------------------------------------------------------------------------
	// Static helper classes
	//---------------------------------------------------------------------------------

	/**
	 * Vertex task information class.
	 */
	public static final class VertexTaskInfo implements Serializable {

		private static final long serialVersionUID = -3090688244400586852L;

		public static final String FIELD_NAME_VERTEX_ID = "vertex_id";

		public static final String FIELD_NAME_TASK_NAME = "task_name";

		public static final String FIELD_NAME_SUBTASK_INDEX = "subtask";

		public static final String FIELD_NAME_SUBTASK_ATTEMPT = "attempt";

		@JsonProperty(FIELD_NAME_VERTEX_ID)
		@JsonSerialize(using = JobVertexIDSerializer.class)
		private final JobVertexID vertexID;

		@JsonProperty(FIELD_NAME_TASK_NAME)
		private final String taskName;

		@JsonProperty(FIELD_NAME_SUBTASK_INDEX)
		private final int subtaskIndex;

		@JsonProperty(FIELD_NAME_SUBTASK_ATTEMPT)
		private final int subtaskAttempt;

		@JsonCreator
		public VertexTaskInfo(
				@JsonProperty(FIELD_NAME_VERTEX_ID) @JsonDeserialize(using = JobVertexIDDeserializer.class)
						JobVertexID vertexID,
				@JsonProperty(FIELD_NAME_TASK_NAME) String taskName,
				@JsonProperty(FIELD_NAME_SUBTASK_INDEX) int subtaskIndex,
				@JsonProperty(FIELD_NAME_SUBTASK_ATTEMPT) int subtaskAttempt) {
			this.vertexID = Preconditions.checkNotNull(vertexID);
			this.taskName = taskName;
			this.subtaskIndex = subtaskIndex;
			this.subtaskAttempt = subtaskAttempt;
		}

		public JobVertexID getVertexID() {
			return vertexID;
		}

		public String getTaskName() {
			return taskName;
		}

		public int getSubtaskIndex() {
			return subtaskIndex;
		}

		public int getSubtaskAttempt() {
			return subtaskAttempt;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (null == o || this.getClass() != o.getClass()) {
				return false;
			}

			VertexTaskInfo that = (VertexTaskInfo) o;
			return Objects.equals(vertexID, that.vertexID) &&
					Objects.equals(taskName, that.taskName) &&
					subtaskIndex == that.subtaskIndex &&
					subtaskAttempt == that.subtaskAttempt;
		}

		@Override
		public int hashCode() {
			return Objects.hash(vertexID, taskName, subtaskIndex, subtaskAttempt);
		}
	}

	/**
	 *
	 */
	public static final class ResourceProfileSerializer extends StdSerializer<ResourceProfile> {

		private static final long serialVersionUID = 1L;

		protected ResourceProfileSerializer() {
			super(ResourceProfile.class);
		}

		@Override
		public void serialize(
				ResourceProfile resourceProfile,
				JsonGenerator jsonGenerator,
				SerializerProvider serializerProvider) throws IOException {
			jsonGenerator.writeStartObject();
			jsonGenerator.writeNumberField(FIELD_NAME_RESOURCE_CPU_CORES, resourceProfile.getCpuCores());
			jsonGenerator.writeNumberField(FIELD_NAME_RESOURCE_HEAP_MEMORY, convertMegabyteToByte(resourceProfile.getHeapMemoryInMB()));
			jsonGenerator.writeNumberField(FIELD_NAME_RESOURCE_DIRECT_MEMORY, convertMegabyteToByte(resourceProfile.getDirectMemoryInMB()));
			jsonGenerator.writeNumberField(FIELD_NAME_RESOURCE_NATIVE_MEMORY, convertMegabyteToByte(resourceProfile.getNativeMemoryInMB()));
			jsonGenerator.writeNumberField(FIELD_NAME_RESOURCE_NETWORK_MEMORY, convertMegabyteToByte(resourceProfile.getNetworkMemoryInMB()));
			jsonGenerator.writeNumberField(FIELD_NAME_RESOURCE_MANAGED_MEMORY, convertMegabyteToByte(resourceProfile.getManagedMemoryInMB()));
			jsonGenerator.writeEndObject();
		}

		private static long convertMegabyteToByte(int value) {
			final long result;

			if (value > 0) {
				result = value * 1024 * 1024;
			} else {
				result = value;
			}

			return result;
		}
	}

	/**
	 *
	 */
	public static final class ResourceProfileDeserializer extends StdDeserializer<ResourceProfile> {

		private static final long serialVersionUID = 1L;

		protected ResourceProfileDeserializer() {
			super(ResourceProfile.class);
		}

		@Override
		public ResourceProfile deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
			JsonNode rootNode = jsonParser.readValueAsTree();

			double cpuCores = rootNode.get(FIELD_NAME_RESOURCE_CPU_CORES).doubleValue();
			int heapMemoryInMB = convertByteToMegabyte(rootNode.get(FIELD_NAME_RESOURCE_HEAP_MEMORY).longValue());
			int directMemoryInMB = convertByteToMegabyte(rootNode.get(FIELD_NAME_RESOURCE_DIRECT_MEMORY).longValue());
			int nativeMemoryInMB = convertByteToMegabyte(rootNode.get(FIELD_NAME_RESOURCE_NATIVE_MEMORY).longValue());
			int networkMemoryInMB = convertByteToMegabyte(rootNode.get(FIELD_NAME_RESOURCE_NETWORK_MEMORY).longValue());
			int managedMemoryInMB = convertByteToMegabyte(rootNode.get(FIELD_NAME_RESOURCE_MANAGED_MEMORY).longValue());

			Map<String, Resource> extendedResources = new HashMap<>();
			if (managedMemoryInMB != 0) {
				extendedResources.put(ResourceSpec.MANAGED_MEMORY_NAME, new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, managedMemoryInMB));
			}

			return new ResourceProfile(
					cpuCores, heapMemoryInMB, directMemoryInMB, nativeMemoryInMB, networkMemoryInMB, extendedResources);
		}

		private static int convertByteToMegabyte(long value) {
			final long result;

			if (value > 0) {
				result = value / (1024 * 1024);
			} else {
				result = value;
			}

			return Long.valueOf(result).intValue();
		}
	}
}
