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

package org.apache.flink.runtime.taskexecutor.exceptions;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.util.Preconditions;

/**
 * Exception which signals that a slot is already occupied by the given
 * {@link AllocationID}.
 */
public class SlotOccupiedException extends SlotAllocationException {
	private static final long serialVersionUID = -3986333914244338888L;

	private final AllocationID allocationId;

	private final JobID jobId;

	public SlotOccupiedException(String message, AllocationID allocationId, JobID jobId) {
		super(message);
		this.allocationId = Preconditions.checkNotNull(allocationId);
		this.jobId = Preconditions.checkNotNull(jobId);
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	public JobID getJobId() {
		return jobId;
	}
}
