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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.executiongraph.IntermediateResult;

import java.util.Collection;

/**
 * Representation of {@link IntermediateResult}.
 */
public interface SchedulingIntermediateDataSet {

	/**
	 * Decrement number of running producers of this intermediate
	 * dataset and get remaining.
	 * @return remaining running producers of the intermediate dataset
	 */
	int decrementNumberOfRunningProducersAndGetRemaining();

	/**
	 * Check whether all the partitions of the intermediate dataset
	 * are finished.
	 * @return whether all the partitions are finished
	 */
	boolean areAllPartitionsFinished();
}
