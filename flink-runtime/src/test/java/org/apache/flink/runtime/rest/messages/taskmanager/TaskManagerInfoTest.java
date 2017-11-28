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
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;

import java.util.Random;
import java.util.UUID;

/**
 * Test for (un)marshalling of the {@link TaskManagerInfo}.
 */
public class TaskManagerInfoTest extends RestResponseMarshallingTestBase<TaskManagerInfo> {

	private static final Random random = new Random();

	@Override
	protected Class<TaskManagerInfo> getTestResponseClass() {
		return TaskManagerInfo.class;
	}

	@Override
	protected TaskManagerInfo getTestResponseInstance() throws Exception {
		return createRandomTaskManagerInfo();
	}

	static TaskManagerInfo createRandomTaskManagerInfo() {
		return new TaskManagerInfo(
			ResourceID.generate(),
			UUID.randomUUID().toString(),
			random.nextInt(),
			random.nextLong(),
			random.nextInt(),
			random.nextInt(),
			new HardwareDescription(
				random.nextInt(),
				random.nextLong(),
				random.nextLong(),
				random.nextLong()));
	}
}
