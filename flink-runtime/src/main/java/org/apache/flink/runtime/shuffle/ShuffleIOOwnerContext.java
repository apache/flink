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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Context of shuffle input/output owner used to create partitions or gates belonging to the owner.
 */
public class ShuffleIOOwnerContext {
	private final String ownerName;
	private final ExecutionAttemptID executionAttemptID;
	private final MetricGroup parentGroup;
	private final MetricGroup outputGroup;
	private final MetricGroup inputGroup;

	public ShuffleIOOwnerContext(
			String ownerName,
			ExecutionAttemptID executionAttemptID,
			MetricGroup parentGroup,
			MetricGroup outputGroup,
			MetricGroup inputGroup) {
		this.ownerName = checkNotNull(ownerName);
		this.executionAttemptID = checkNotNull(executionAttemptID);
		this.parentGroup = checkNotNull(parentGroup);
		this.outputGroup = checkNotNull(outputGroup);
		this.inputGroup = checkNotNull(inputGroup);
	}

	public String getOwnerName() {
		return ownerName;
	}

	public ExecutionAttemptID getExecutionAttemptID() {
		return executionAttemptID;
	}

	public MetricGroup getParentGroup() {
		return parentGroup;
	}

	public MetricGroup getOutputGroup() {
		return outputGroup;
	}

	public MetricGroup getInputGroup() {
		return inputGroup;
	}
}
