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

package org.apache.flink.runtime.rest.messages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests that the {@link SubtasksTimesInfo} can be marshalled and unmarshalled.
 */
public class SubtasksTimesInfoTest extends RestResponseMarshallingTestBase<SubtasksTimesInfo> {

	@Override
	protected Class<SubtasksTimesInfo> getTestResponseClass() {
		return SubtasksTimesInfo.class;
	}

	@Override
	protected SubtasksTimesInfo getTestResponseInstance() throws Exception {
		List<SubtasksTimesInfo.SubtaskTimeInfo> subtasks = new ArrayList<>();

		Map<String, Long> subTimeMap1 = new HashMap<>();
		subTimeMap1.put("state11", System.currentTimeMillis());
		subTimeMap1.put("state12", System.currentTimeMillis());
		subTimeMap1.put("state13", System.currentTimeMillis());
		subtasks.add(new SubtasksTimesInfo.SubtaskTimeInfo(0, "local1", 1L, subTimeMap1));

		Map<String, Long> subTimeMap2 = new HashMap<>();
		subTimeMap1.put("state21", System.currentTimeMillis());
		subTimeMap1.put("state22", System.currentTimeMillis());
		subTimeMap1.put("state23", System.currentTimeMillis());
		subtasks.add(new SubtasksTimesInfo.SubtaskTimeInfo(1, "local2", 2L, subTimeMap2));

		Map<String, Long> subTimeMap3 = new HashMap<>();
		subTimeMap1.put("state31", System.currentTimeMillis());
		subTimeMap1.put("state32", System.currentTimeMillis());
		subTimeMap1.put("state33", System.currentTimeMillis());
		subtasks.add(new SubtasksTimesInfo.SubtaskTimeInfo(2, "local3", 3L, subTimeMap3));

		return new SubtasksTimesInfo("testId", "testName", System.currentTimeMillis(), subtasks);
	}
}
