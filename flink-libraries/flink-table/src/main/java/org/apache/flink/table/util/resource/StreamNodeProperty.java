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

package org.apache.flink.table.util.resource;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.streaming.api.graph.StreamNode;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * StreamNode properties.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamNodeProperty extends AbstractJsonSerializable implements Comparable<StreamNodeProperty> {
	private String uid;
	private String name = "";
	private String pact = "";
	private String slotSharingGroup = "default";

	@JsonProperty("parallelism")
	private int parallelism;

	@JsonProperty("maxParallelism")
	private int maxParallelism;

	@JsonProperty("vcore")
	private double cpuCores;

	@JsonProperty("heap_memory")
	private int heapMemoryInMB;

	@JsonProperty("direct_memory")
	private int directMemoryInMB;

	@JsonProperty("native_memory")
	private int nativeMemoryInMB;

	@JsonProperty("managed_memory")
	private int managedMemoryInMB;

	@JsonProperty("floating_managed_memory")
	private int floatingManagedMemoryInMB;

	@JsonProperty("gpu")
	private double gpuLoad;

	@JsonProperty("otherResources")
	private Map<String, Double> otherResources;

	public StreamNodeProperty() {
	}

	public StreamNodeProperty(String uid) {
		this.uid = uid;
	}

	public void update(StreamNodeProperty streamNodeProperties) {
		this.uid = streamNodeProperties.getUid();
		this.parallelism = streamNodeProperties.getParallelism();
		this.maxParallelism = streamNodeProperties.getMaxParallelism();
		this.slotSharingGroup = streamNodeProperties.getSlotSharingGroup();
		this.cpuCores = streamNodeProperties.getCpuCores();
		this.heapMemoryInMB = streamNodeProperties.getHeapMemoryInMB();
		this.directMemoryInMB = streamNodeProperties.getDirectMemoryInMB();
		this.nativeMemoryInMB = streamNodeProperties.getNativeMemoryInMB();
		this.managedMemoryInMB = streamNodeProperties.getManagedMemoryInMB();
		this.floatingManagedMemoryInMB = streamNodeProperties.getFloatingManagedMemoryInMB();

		this.gpuLoad = streamNodeProperties.getGpuLoad();
		if (streamNodeProperties.otherResources != null) {
			this.otherResources = new HashMap<>(streamNodeProperties.otherResources);
		}
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSlotSharingGroup() {
		return slotSharingGroup;
	}

	public int getParallelism() {
		return parallelism;
	}

	public StreamNodeProperty setParallelism(int parallelism) {
		this.parallelism = parallelism;
		return this;
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	public void setMaxParallelism(int maxParallelism) {
		this.maxParallelism = maxParallelism;
	}

	public double getCpuCores() {
		return cpuCores;
	}

	public void setCpuCores(double cpuCores) {
		this.cpuCores = cpuCores;
	}

	public int getHeapMemoryInMB() {
		return heapMemoryInMB;
	}

	public void setHeapMemoryInMB(int heapMemoryInMB) {
		this.heapMemoryInMB = heapMemoryInMB;
	}

	public int getDirectMemoryInMB() {
		return directMemoryInMB;
	}

	public void setDirectMemoryInMB(int directMemoryInMB) {
		this.directMemoryInMB = directMemoryInMB;
	}

	public int getNativeMemoryInMB() {
		return nativeMemoryInMB;
	}

	public void setNativeMemoryInMB(int nativeMemoryInMB) {
		this.nativeMemoryInMB = nativeMemoryInMB;
	}

	public double getGpuLoad() {
		return gpuLoad;
	}

	public void setGpuLoad(double gpuLoad) {
		this.gpuLoad = gpuLoad;
	}

	@Override
	public int compareTo(StreamNodeProperty streamNodeProperty) {
		return this.uid.compareTo(streamNodeProperty.getUid());
	}

	public void apple(StreamNode node) {
		// CONSIDER: find a better way to identify transformation with StreamNode, so that we can better
		// detect mismatch between JSON and stream graph.
		if (node != null) {
			node.setParallelism(parallelism);
			StreamNodeUtil.setMaxParallelism(node, maxParallelism);

			ResourceSpec.Builder builder = ResourceSpec.newBuilder()
					.setCpuCores(cpuCores)
					.setHeapMemoryInMB(heapMemoryInMB)
					.setDirectMemoryInMB(directMemoryInMB)
					.setNativeMemoryInMB(nativeMemoryInMB);

			if (gpuLoad > 0) {
				builder.setGPUResource(gpuLoad);
			}

			if (otherResources != null) {
				for (Map.Entry<String, Double> entry : otherResources.entrySet()) {
					builder.addExtendedResource(new CommonExtendedResource(entry.getKey(), entry.getValue()));
				}
			}
			if (floatingManagedMemoryInMB > 0) {
				builder.addExtendedResource(new CommonExtendedResource(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME, floatingManagedMemoryInMB));
			}
			if (node.getMinResources().getExtendedResources().containsKey(ResourceSpec.MANAGED_MEMORY_NAME)) {
				builder.addExtendedResource(new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, node.getMinResources().getExtendedResources().get(ResourceSpec.MANAGED_MEMORY_NAME).getValue()));
			}

			ResourceSpec resourceSpec = builder.build();
			node.setResources(resourceSpec, resourceSpec);
		}
	}

	public int getFloatingManagedMemoryInMB() {
		return floatingManagedMemoryInMB;
	}

	public void setFloatingManagedMemoryInMB(int floatingManagedMemoryInMB) {
		this.floatingManagedMemoryInMB = floatingManagedMemoryInMB;
	}

	public int getManagedMemoryInMB() {
		return managedMemoryInMB;
	}

	public void setManagedMemoryInMB(int managedMemoryInMB) {
		this.managedMemoryInMB = managedMemoryInMB;
	}

	public String getPact() {
		return pact;
	}

	public void setPact(String pact) {
		this.pact = pact;
	}
}

