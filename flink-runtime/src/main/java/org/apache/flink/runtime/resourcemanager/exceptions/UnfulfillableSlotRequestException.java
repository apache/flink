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

package org.apache.flink.runtime.resourcemanager.exceptions;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;


/**
 * Exception denoting that a slot request can not be fulfilled by any slot in the cluster.
 * This usually indicates that the slot request should not be pended or retried.
 */
public class UnfulfillableSlotRequestException extends ResourceManagerException {
	private static final long serialVersionUID = 4453490263648758730L;

	public UnfulfillableSlotRequestException(AllocationID allocationId, ResourceProfile resourceProfile) {
		super("Could not fulfill slot request " + allocationId + ". "
			+ "Requested resource profile (" + resourceProfile + ") is unfulfillable.");
	}
}
