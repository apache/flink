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

import org.apache.flink.runtime.io.network.partition.DataSetMetaInfo;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link ResponseBody} for {@link ClusterDataSetListHeaders}.
 */
public class ClusterDataSetListResponseBody implements ResponseBody {
	private static final String FIELD_NAME_PARTITIONS = "dataSets";

	@JsonProperty(FIELD_NAME_PARTITIONS)
	private final List<ClusterDataSetEntry> dataSets;

	@JsonCreator
	private ClusterDataSetListResponseBody(@JsonProperty(FIELD_NAME_PARTITIONS) List<ClusterDataSetEntry> dataSets) {
		this.dataSets = dataSets;
	}

	public static ClusterDataSetListResponseBody from(Map<IntermediateDataSetID, DataSetMetaInfo> dataSets) {
		final List<ClusterDataSetEntry> convertedInfo = dataSets.entrySet().stream()
			.map(entry -> {
				DataSetMetaInfo metaInfo = entry.getValue();
				int numRegisteredPartitions = metaInfo.getNumRegisteredPartitions().orElse(0);
				int numTotalPartition = metaInfo.getNumTotalPartitions();
				return new ClusterDataSetEntry(entry.getKey(), numRegisteredPartitions == numTotalPartition);
			})
			.collect(Collectors.toList());
		return new ClusterDataSetListResponseBody(convertedInfo);
	}

	@JsonIgnore
	public List<ClusterDataSetEntry> getDataSets() {
		return dataSets;
	}
}
