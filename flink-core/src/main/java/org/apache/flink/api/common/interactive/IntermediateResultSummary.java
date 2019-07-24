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
 *  This helps keep track of the result partition descriptors of a job.
 *  The descriptor of a result partition contains locations and other meta data for consuming.
 *  An instance of IntermediateResultSummary will be created and sent back to ExecutionEnvironment after job finishing.
 *  ExecutionEnvironment also have an instance of IntermediateResultSummary for combining all the returned IntermediateResultSummary,
 *  so the ExecutionEnvironment can keep track of all IntermediateResultSummaries which created by its submitted jobs.
 *  As for InteractiveProgramming, the SQL Planner/Optimizer can make use of this summary and decide whether to reuse the IntermediateResult.
 *
 *  @param <IR> Type for identify an intermediate result.
 *  @param <DESC> Type for identify the descriptors of intermediate results, the descriptors may contains both partitions and locations of all intermediate results.
 */
public interface IntermediateResultSummary<IR, DESC> extends Serializable {

	/**
	 * Return the descriptors of all the intermediate results.
	 *
	 * @return descriptors of all the intermediate results.
	 */
	DESC getIntermediateResultDescriptors();

	/**
	 * Some descriptors may be missing if any error occurs while collecting the description of the intermediate result.
	 * We keep track of this ids so the client can decide what to do.
	 * Generally a result partition delete request will be proposed.
	 *
	 * @return incomplete IntermediateDataSet ids
	 */
	Set<IR> getIncompleteIntermediateDataSetIds();

	/**
	 * Merge IntermediateResultDescriptors and IncompleteIntermediateDataSetIds from another IntermediateResultSummary.
	 * After the merge action, getIntermediateResultDescriptors and getIncompleteIntermediateDataSetIds should return merged results.
	 * @param summary, another IntermediateResultSummary of same type.
	 */
	void merge(IntermediateResultSummary<IR, DESC> summary);
}
