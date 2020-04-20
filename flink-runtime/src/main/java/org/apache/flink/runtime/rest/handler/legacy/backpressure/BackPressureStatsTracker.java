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

package org.apache.flink.runtime.rest.handler.legacy.backpressure;

import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.util.FlinkException;

import java.util.Optional;

/**
 * Interface for a tracker of back pressure statistics for {@link ExecutionJobVertex}.
 */
public interface BackPressureStatsTracker {

	/**
	 * Returns back pressure statistics for an operator. Automatically triggers back pressure request
	 * if statistics are not available or outdated.
	 *
	 * @param vertex Operator to get the stats for.
	 * @return Back pressure statistics for an operator
	 */
	Optional<OperatorBackPressureStats> getOperatorBackPressureStats(ExecutionJobVertex vertex);

	/**
	 * Cleans up the operator stats cache if it contains timed out entries.
	 *
	 * <p>The Guava cache only evicts as maintenance during normal operations.
	 * If this handler is inactive, it will never be cleaned.
	 */
	void cleanUpOperatorStatsCache();

	/**
	 * Shuts the BackPressureStatsTracker down.
	 *
	 * @throws FlinkException if the BackPressureStatsTracker could not be shut down
	 */
	void shutDown() throws FlinkException;
}
