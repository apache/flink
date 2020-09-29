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

package org.apache.flink.runtime.slots;

import org.apache.flink.api.common.JobID;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Represents the total resource requirements for a job, and the information required to connect to the corresponding
 * job master.
 */
public class ResourceRequirements implements Serializable {

	private static final long serialVersionUID = 1L;

	private final JobID jobId;

	private final String targetAddress;

	private final Collection<ResourceRequirement> resourceRequirements;

	private ResourceRequirements(JobID jobId, String targetAddress, Collection<ResourceRequirement> resourceRequirements) {
		this.jobId = Preconditions.checkNotNull(jobId);
		this.targetAddress = Preconditions.checkNotNull(targetAddress);
		this.resourceRequirements = Preconditions.checkNotNull(resourceRequirements);
	}

	public JobID getJobId() {
		return jobId;
	}

	public String getTargetAddress() {
		return targetAddress;
	}

	public Collection<ResourceRequirement> getResourceRequirements() {
		return resourceRequirements;
	}

	public static ResourceRequirements create(JobID jobId, String targetAddress, Collection<ResourceRequirement> resourceRequirements) {
		return new ResourceRequirements(jobId, targetAddress, resourceRequirements);
	}

	public static ResourceRequirements empty(JobID jobId, String targetAddress) {
		return new ResourceRequirements(jobId, targetAddress, Collections.emptyList());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ResourceRequirements that = (ResourceRequirements) o;
		return Objects.equals(jobId, that.jobId) &&
			Objects.equals(targetAddress, that.targetAddress) &&
			Objects.equals(resourceRequirements, that.resourceRequirements);
	}

	@Override
	public int hashCode() {
		return Objects.hash(jobId, targetAddress, resourceRequirements);
	}

	@Override
	public String toString() {
		return "ResourceRequirements{" +
			"jobId=" + jobId +
			", targetAddress='" + targetAddress + '\'' +
			", resourceRequirements=" + resourceRequirements +
			'}';
	}
}

