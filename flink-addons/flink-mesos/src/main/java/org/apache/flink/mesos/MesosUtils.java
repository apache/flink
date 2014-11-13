/**
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

package org.apache.flink.mesos;

import com.google.protobuf.ByteString;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;

import java.util.HashMap;
import java.util.Map;

public class MesosUtils {
	public static void setTaskState(final ExecutorDriver executorDriver, final Protos.TaskID taskID, final Protos.TaskState newState) {
		Protos.TaskStatus state = Protos.TaskStatus.newBuilder()
				.setTaskId(taskID)
				.setState(newState).build();

		executorDriver.sendStatusUpdate(state);
	}

	public static Protos.ExecutorInfo createExecutorInfo(String id, String name, String command) {
		return Protos.ExecutorInfo.newBuilder()
				.setExecutorId(Protos.ExecutorID.newBuilder().setValue(id))
				.setCommand(Protos.CommandInfo.newBuilder().setValue(command))
				.setName(name)
				.setData(ByteString.copyFromUtf8("Hello World"))
				.build();
	}

	public static Protos.TaskInfo createTaskInfo(String name, HashMap<String, Double> resources, Protos.ExecutorInfo exinfo, Protos.SlaveID slaveID, Protos.TaskID taskID) {
		Protos.TaskInfo.Builder taskInfo = Protos.TaskInfo.newBuilder()
				.setName(name)
				.setTaskId(taskID)
				.setSlaveId(slaveID)
				.setExecutor(exinfo);
		for (Map.Entry<String, Double> resource: resources.entrySet()) {
			taskInfo.addResources(Protos.Resource.newBuilder()
					.setName(resource.getKey())
					.setType(Protos.Value.Type.SCALAR)
					.setScalar(Protos.Value.Scalar.newBuilder().setValue(resource.getValue())));
		}

		return taskInfo.build();
	}
}
