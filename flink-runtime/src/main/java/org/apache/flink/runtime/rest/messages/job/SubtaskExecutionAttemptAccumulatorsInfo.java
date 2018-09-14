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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.rest.handler.job.SubtaskExecutionAttemptAccumulatorsHandler;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Objects;

/**
 * Response type of the {@link SubtaskExecutionAttemptAccumulatorsHandler}.
 */
public class SubtaskExecutionAttemptAccumulatorsInfo implements ResponseBody {

	public static final String FIELD_NAME_SUBTASK_INDEX = "subtask";
	public static final String FIELD_NAME_ATTEMPT_NUM = "attempt";
	public static final String FIELD_NAME_ID = "id";
	public static final String FIELD_NAME_USER_ACCUMULATORS = "user-accumulators";

	@JsonProperty(FIELD_NAME_SUBTASK_INDEX)
	private final int subtaskIndex;

	@JsonProperty(FIELD_NAME_ATTEMPT_NUM)
	private final int attemptNum;

	@JsonProperty(FIELD_NAME_ID)
	private final String id;

	@JsonProperty(FIELD_NAME_USER_ACCUMULATORS)
	private final Collection<UserAccumulator> userAccumulatorList;

	@JsonCreator
	public SubtaskExecutionAttemptAccumulatorsInfo(
			@JsonProperty(FIELD_NAME_SUBTASK_INDEX) int subtaskIndex,
			@JsonProperty(FIELD_NAME_ATTEMPT_NUM) int attemptNum,
			@JsonProperty(FIELD_NAME_ID) String id,
			@JsonProperty(FIELD_NAME_USER_ACCUMULATORS) Collection<UserAccumulator> userAccumulatorList) {

		this.subtaskIndex = Preconditions.checkNotNull(subtaskIndex);
		this.attemptNum = Preconditions.checkNotNull(attemptNum);
		this.id = Preconditions.checkNotNull(id);
		this.userAccumulatorList = Preconditions.checkNotNull(userAccumulatorList);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SubtaskExecutionAttemptAccumulatorsInfo that = (SubtaskExecutionAttemptAccumulatorsInfo) o;
		return subtaskIndex == that.subtaskIndex &&
			attemptNum == that.attemptNum &&
			Objects.equals(id, that.id) &&
			Objects.equals(userAccumulatorList, that.userAccumulatorList);
	}

	@Override
	public int hashCode() {
		return Objects.hash(subtaskIndex, attemptNum, id, userAccumulatorList);
	}
}
