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
import java.util.Collections;
import java.util.List;

/**
 * Tests that the {@link JobAccumulatorsInfo} can be marshalled and unmarshalled.
 */
public class JobAccumulatorsInfoTest extends RestResponseMarshallingTestBase<JobAccumulatorsInfo> {
	@Override
	protected Class<JobAccumulatorsInfo> getTestResponseClass() {
		return JobAccumulatorsInfo.class;
	}

	@Override
	protected JobAccumulatorsInfo getTestResponseInstance() throws Exception {
		List<JobAccumulatorsInfo.UserTaskAccumulator> userAccumulatorList = new ArrayList<>(3);
		userAccumulatorList.add(new JobAccumulatorsInfo.UserTaskAccumulator(
			"uta1.name",
			"uta1.type",
			"uta1.value"));
		userAccumulatorList.add(new JobAccumulatorsInfo.UserTaskAccumulator(
			"uta2.name",
			"uta2.type",
			"uta2.value"));
		userAccumulatorList.add(new JobAccumulatorsInfo.UserTaskAccumulator(
			"uta3.name",
			"uta3.type",
			"uta3.value"));

		return new JobAccumulatorsInfo(Collections.emptyList(), userAccumulatorList, Collections.emptyMap());
	}
}
