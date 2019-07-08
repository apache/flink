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

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * This interface keeps track of the mapping of IntermediateDataSetID -> (ResultPartitionID, ShuffleDescriptor).
 */
public interface PersistentIntermediateResultDescriptor extends Serializable {

	/**
	 * Mapping of IntermediateDataSetID -> (ResultPartitionID, ShuffleDescriptor).
	 */
	Map<AbstractID, Map<AbstractID, SerializedValue<Object>>> getPersistentShuffleDescriptors();

	/**
	 * If a ShuffleDescriptor of one IntermediateDataSetID is missing, we keep track of this IntermediateDataSetID.
	 */
	Set<AbstractID> getIncompleteIntermediateDataSetIds();

	/**
	 * Merge another Descriptor.
	 */
	void mergeDescriptor(PersistentIntermediateResultDescriptor newPersistentShuffleDescriptor);
}
