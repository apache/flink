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

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

/**
 * Tracks the partition location to generate {@link ResultPartitionLocation} when deploying tasks
 * or updating input channels.
 */
public interface ResultPartitionLocationTracker {
	/**
	 * Gets the result partition location.
	 *
	 * @param producerLocation The producer location of the result partition.
	 * @param consumerLocation The consumer location of the result partition.
	 * @param intermediateResult The intermediate result to be consumed.
	 * @return the Result partition location.
	 */
	ResultPartitionLocation getResultPartitionLocation(
		TaskManagerLocation producerLocation,
		TaskManagerLocation consumerLocation,
		IntermediateResult intermediateResult);
}
