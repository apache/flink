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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.util.AbstractID;

import java.io.Serializable;

/**
 * Class for Resource Ids assigned at the FlinkResourceManager.
 */
public class ResourceID implements Serializable {

	private static final long serialVersionUID = 42L;

	private final String resourceId;

	public ResourceID(String resourceId) {
		this.resourceId = resourceId;
	}

	public String getResourceId() {
		return resourceId;
	}

	/**
	 * Generate a random resource id.
	 * @return A random resource id.
	 */
	public static ResourceID generate() {
		return new ResourceID(new AbstractID().toString());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o == null || getClass() != o.getClass()) {
			return false;
		} else {
			return resourceId.equals(((ResourceID) o).resourceId);
		}
	}

	@Override
	public int hashCode() {
		return resourceId.hashCode();
	}

	@Override
	public String toString() {
		return "ResourceID{" +
			"resourceId='" + resourceId + '\'' +
			'}';
	}
}
