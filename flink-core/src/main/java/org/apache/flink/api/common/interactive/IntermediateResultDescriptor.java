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

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 *  This helps keep track of the shuffle descriptors of a job, and we only collect BLOCKING_PERSISTENT type for now.
 *  IntermediateResultDescriptor will be created when a job finishes and sent back to ExecutionEnvironment.
 *  ExecutionEnvironment also have an instance of IntermediateResultDescriptor, the mergeDescriptor method will be called
 *  when an IntermediateResultDescriptor is sent back to ExecutionEnvironment, so the ExecutionEnvironment can keep all the
 *  IntermediateResultDescriptors which created by its submitted jobs.
 *  As for InteractiveProgramming, the Planner can check the shuffle result existence for a sub-tree and decide whether to reuse the shuffle result.
 *
 *  @param <IR> Type for identify an intermediate result.
 *  @param <DESC> Type for identify the description of the intermediate result.
 */
public interface IntermediateResultDescriptor<IR, DESC> extends Serializable {

	/**
	 * Return the mapping from intermediate result to its descriptions.
	 *
	 * @return intermediate result to its descriptions.
	 */
	Map<IR, DESC> getIntermediateResultDescriptions();

	/**
	 * Some description may be missing if any error occurs in collecting the .
	 * We keep track of this intermediate result so the client can decide what to do.
	 * Generally a result partition delete request will be proposed.
	 *
	 * @return incomplete IntermediateDataSets
	 */
	Set<IR> getIncompleteIntermediateDataSets();

	/**
	 * Merge another Descriptor, this helps to combine other intermediate descriptors that created by other job.
	 * The implementation should add other IntermediateResultDescriptions and IncompleteIntermediateDataSets to itself.
	 *
	 * @param other the other IntermediateResultDescriptor created by some other job.
	 */
	void mergeDescriptor(IntermediateResultDescriptor<IR, DESC> other);
}
