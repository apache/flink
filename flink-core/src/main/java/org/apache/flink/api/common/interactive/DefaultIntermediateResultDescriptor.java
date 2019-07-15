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

import org.apache.flink.util.AbstractID;
import org.apache.flink.util.SerializedValue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This is the default implementation of IntermediateResultDescriptor.
 * See {@link IntermediateResultDescriptor} for more details.
 */
public class DefaultIntermediateResultDescriptor
	implements IntermediateResultDescriptor<AbstractID, Map<AbstractID, SerializedValue<Object>>> {

	private final Map<AbstractID, Map<AbstractID, SerializedValue<Object>>> intermediateResultDescriptors = new HashMap<>();

	private final Set<AbstractID> incompleteIntermediateDataSetIds = new HashSet<>();

	public DefaultIntermediateResultDescriptor() {}

	public DefaultIntermediateResultDescriptor(
		Map<AbstractID, Map<AbstractID, SerializedValue<Object>>> intermediateResultDescriptors,
		Set<AbstractID> incompleteIntermediateDataSetIds) {

		this.intermediateResultDescriptors.putAll(intermediateResultDescriptors);
		this.incompleteIntermediateDataSetIds.addAll(incompleteIntermediateDataSetIds);
	}

	/**
	 * Return the mapping from intermediate result to its (ResultPartitionID, ShuffleDescriptor) tuples.
	 * We use AbstractID here due to package visibility, and the ShuffleDescriptor are serialized in form of
	 * SerializedValue, the deserialization will only be triggered in JM before an Execution.
	 *
	 * @return Mapping from IntermediateDataSetID to its (ResultPartitionID, ShuffleDescriptor) tuples.
	 */
	@Override
	public Map<AbstractID, Map<AbstractID, SerializedValue<Object>>> getIntermediateResultDescriptions() {
		return intermediateResultDescriptors;
	}

	@Override
	public Set<AbstractID> getIncompleteIntermediateDataSets() {
		return incompleteIntermediateDataSetIds;
	}

	@Override
	public void mergeDescriptor(IntermediateResultDescriptor<AbstractID, Map<AbstractID, SerializedValue<Object>>> newPersistentShuffleDescriptor) {
		intermediateResultDescriptors.putAll(newPersistentShuffleDescriptor.getIntermediateResultDescriptions());
		incompleteIntermediateDataSetIds.addAll(newPersistentShuffleDescriptor.getIncompleteIntermediateDataSets());
	}
}
