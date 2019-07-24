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

package org.apache.flink.api.common.interactive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.SerializedValue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class helps to keep both ResultPartitionId and locations(in form of serialized ShuffleDescriptor) of IntermediateResults.
 */
@Internal
public class IntermediateResultDescriptors implements Serializable {

	// Mappings from IntermediateDataSetID to its ResultPartition ids and locations.
	private final Map<AbstractID, Map<AbstractID, SerializedValue<Object>>> intermediateResultDescriptors;

	// Set contains incomplete IntermediateDataSetID
	private final Set<AbstractID> incompleteIntermediateDataSetIds;

	public IntermediateResultDescriptors() {
		this(new HashMap<>(), new HashSet<>());
	}

	public IntermediateResultDescriptors(Map<AbstractID, Map<AbstractID, SerializedValue<Object>>> intermediateResultDescriptors,
										 Set<AbstractID> incompleteIntermediateDataSetIds) {
		this.intermediateResultDescriptors = intermediateResultDescriptors;
		this.incompleteIntermediateDataSetIds = incompleteIntermediateDataSetIds;
	}

	public Map<AbstractID, Map<AbstractID, SerializedValue<Object>>> getIntermediateResultDescriptors() {
		return intermediateResultDescriptors;
	}

	public Set<AbstractID> getIncompleteIntermediateDataSetIds() {
		return incompleteIntermediateDataSetIds;
	}

}
