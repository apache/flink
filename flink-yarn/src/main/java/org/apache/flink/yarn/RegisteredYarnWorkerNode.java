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

package org.apache.flink.yarn;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;

import org.apache.hadoop.yarn.api.records.Container;

import static java.util.Objects.requireNonNull;

/**
 * A representation of a registered Yarn container managed by the {@link YarnFlinkResourceManager}.
 */
public class RegisteredYarnWorkerNode implements ResourceIDRetrievable {

	/** The container on which the worker runs. */
	private final Container yarnContainer;

	/** The resource id associated with this worker type. */
	private final ResourceID resourceID;

	public RegisteredYarnWorkerNode(Container yarnContainer) {
		this.yarnContainer = requireNonNull(yarnContainer);
		this.resourceID = YarnFlinkResourceManager.extractResourceID(yarnContainer);
	}

	public Container yarnContainer() {
		return yarnContainer;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "RegisteredYarnWorkerNode{" +
			"yarnContainer=" + yarnContainer +
			'}';
	}

	@Override
	public ResourceID getResourceID() {
		return resourceID;
	}
}
