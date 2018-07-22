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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for responses from the ResourceManager to a registration attempt by a JobMaster.
 */
public class JobMasterRegistrationSuccess extends RegistrationResponse.Success {

	private static final long serialVersionUID = 5577641250204140415L;

	private final ResourceManagerId resourceManagerId;

	private final ResourceID resourceManagerResourceId;

	public JobMasterRegistrationSuccess(
			final ResourceManagerId resourceManagerId,
			final ResourceID resourceManagerResourceId) {
		this.resourceManagerId = checkNotNull(resourceManagerId);
		this.resourceManagerResourceId = checkNotNull(resourceManagerResourceId);
	}

	public ResourceManagerId getResourceManagerId() {
		return resourceManagerId;
	}

	public ResourceID getResourceManagerResourceId() {
		return resourceManagerResourceId;
	}

	@Override
	public String toString() {
		return "JobMasterRegistrationSuccess{" +
			"resourceManagerId=" + resourceManagerId +
			", resourceManagerResourceId=" + resourceManagerResourceId +
			'}';
	}
}
