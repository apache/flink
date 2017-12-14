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
import org.apache.flink.runtime.rest.messages.MessagePathParameter;

/**
 * TaskManager id path parameter used by TaskManager related handlers.
 */
public class TaskManagerIdPathParameter extends MessagePathParameter<ResourceID> {

	public static final String KEY = "taskmanagerid";

	public TaskManagerIdPathParameter() {
		super(KEY);
	}

	@Override
	protected ResourceID convertFromString(String value) {
		return new ResourceID(value);
	}

	@Override
	protected String convertToString(ResourceID value) {
		return value.getResourceIdString();
	}
}
