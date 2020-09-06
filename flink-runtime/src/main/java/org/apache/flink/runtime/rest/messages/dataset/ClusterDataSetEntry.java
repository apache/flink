/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.dataset;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The entry for a single cluster data set.
 *
 * @see ClusterDataSetListResponseBody
 */
class ClusterDataSetEntry {
	private static final String FIELD_NAME_DATA_SET_ID = "id";
	private static final String FIELD_NAME_COMPLETE = "isComplete";

	@JsonProperty(FIELD_NAME_DATA_SET_ID)
	private final String dataSetId;

	@JsonProperty(FIELD_NAME_COMPLETE)
	private final boolean isComplete;

	ClusterDataSetEntry(IntermediateDataSetID dataSetId, boolean isComplete) {
		this(dataSetId.toHexString(), isComplete);
	}

	@JsonCreator
	private ClusterDataSetEntry(
		@JsonProperty(FIELD_NAME_DATA_SET_ID) String dataSetId,
		@JsonProperty(FIELD_NAME_COMPLETE) boolean isComplete) {
		this.dataSetId = dataSetId;
		this.isComplete = isComplete;
	}

	@JsonIgnore
	public String getDataSetId() {
		return dataSetId;
	}

	@JsonIgnore
	public boolean isComplete() {
		return isComplete;
	}
}
