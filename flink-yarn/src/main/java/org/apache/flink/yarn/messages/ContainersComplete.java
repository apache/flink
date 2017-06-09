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

package org.apache.flink.yarn.messages;

import org.apache.flink.yarn.YarnFlinkResourceManager;

import org.apache.hadoop.yarn.api.records.ContainerStatus;

import java.util.List;

/**
 * Message sent by the callback handler to the {@link YarnFlinkResourceManager}
 * to notify it that a set of new containers is complete.
 *
 * <p>NOTE: This message is not serializable, because the ContainerStatus object is not serializable.
 */
public class ContainersComplete {

	private final List<ContainerStatus> containers;

	public ContainersComplete(List<ContainerStatus> containers) {
		this.containers = containers;
	}

	public List<ContainerStatus> containers() {
		return containers;
	}

	@Override
	public String toString() {
		return "ContainersComplete: " + containers;
	}
}
