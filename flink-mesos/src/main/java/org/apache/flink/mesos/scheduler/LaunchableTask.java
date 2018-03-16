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

package org.apache.flink.mesos.scheduler;

import org.apache.flink.mesos.util.MesosResourceAllocation;

import com.netflix.fenzo.TaskRequest;
import org.apache.mesos.Protos;

/**
 * Specifies the task requirements and produces a Mesos TaskInfo description.
 */
public interface LaunchableTask {

	/**
	 * Get a representation of the task requirements as understood by Fenzo.
     */
	TaskRequest taskRequest();

	/**
	 * Prepare to launch the task by producing a Mesos TaskInfo record.
	 * @param slaveId the slave assigned to the task.
	 * @param allocation the resource allocation to take from.
     * @return a TaskInfo.
     */
	Protos.TaskInfo launch(Protos.SlaveID slaveId, MesosResourceAllocation allocation);
}
