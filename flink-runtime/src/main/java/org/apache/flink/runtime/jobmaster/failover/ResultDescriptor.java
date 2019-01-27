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

package org.apache.flink.runtime.jobmaster.failover;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.io.Serializable;

/**
 * Descriptor for the result partition. Used for persist the result information of a finished execution.
 */
public class ResultDescriptor implements Serializable {

	private TaskManagerLocation taskManagerLocation;

	private ResultPartitionID[] resultPartitionIds;

	public ResultDescriptor(TaskManagerLocation taskManagerLocation, ResultPartitionID[] resultPartitionIds) {
		this.taskManagerLocation = taskManagerLocation;
		this.resultPartitionIds = resultPartitionIds;
	}

	public TaskManagerLocation getTaskManagerLocation() {
		return taskManagerLocation;
	}

	public ResultPartitionID[] getResultPartitionIds() {
		return resultPartitionIds;
	}

}
