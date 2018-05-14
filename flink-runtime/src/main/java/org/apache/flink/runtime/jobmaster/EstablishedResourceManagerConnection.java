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
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;

import javax.annotation.Nonnull;

/**
 * Class which contains the connection details of an established
 * connection with the ResourceManager.
 */
class EstablishedResourceManagerConnection {

	private final ResourceManagerGateway resourceManagerGateway;

	private final ResourceManagerId resourceManagerId;

	private final ResourceID resourceManagerResourceID;

	EstablishedResourceManagerConnection(
			@Nonnull ResourceManagerGateway resourceManagerGateway,
			@Nonnull ResourceManagerId resourceManagerId,
			@Nonnull ResourceID resourceManagerResourceID) {
		this.resourceManagerGateway = resourceManagerGateway;
		this.resourceManagerId = resourceManagerId;
		this.resourceManagerResourceID = resourceManagerResourceID;
	}

	public ResourceManagerGateway getResourceManagerGateway() {
		return resourceManagerGateway;
	}

	public ResourceManagerId getResourceManagerId() {
		return resourceManagerId;
	}

	public ResourceID getResourceManagerResourceID() {
		return resourceManagerResourceID;
	}
}
