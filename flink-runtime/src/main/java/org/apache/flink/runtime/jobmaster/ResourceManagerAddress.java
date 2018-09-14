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

import org.apache.flink.runtime.resourcemanager.ResourceManagerId;

import javax.annotation.Nonnull;

import java.util.Objects;

/**
 * Current address and fencing token of the leading ResourceManager.
 */
public class ResourceManagerAddress {

	@Nonnull
	private final String address;

	@Nonnull
	private final ResourceManagerId resourceManagerId;

	public ResourceManagerAddress(@Nonnull String address, @Nonnull ResourceManagerId resourceManagerId) {
		this.address = address;
		this.resourceManagerId = resourceManagerId;
	}

	@Nonnull
	public String getAddress() {
		return address;
	}

	@Nonnull
	public ResourceManagerId getResourceManagerId() {
		return resourceManagerId;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}

		ResourceManagerAddress that = (ResourceManagerAddress) obj;
		return Objects.equals(address, that.address) &&
			Objects.equals(resourceManagerId, that.resourceManagerId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(address, resourceManagerId);
	}

	@Override
	public String toString() {
		return address + '(' + resourceManagerId + ')';
	}
}
