/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.rpc;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;

import eu.stratosphere.nephele.deployment.ChannelDeploymentDescriptor;
import eu.stratosphere.nephele.deployment.GateDeploymentDescriptor;
import eu.stratosphere.nephele.deployment.TaskDeploymentDescriptor;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.local.LocalInstance;
import eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitWrapper;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.TaskCheckpointState;
import eu.stratosphere.nephele.taskmanager.TaskExecutionState;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;
import eu.stratosphere.nephele.taskmanager.routing.ConnectionInfoLookupResponse;
import eu.stratosphere.nephele.taskmanager.routing.RemoteReceiver;

/**
 * This utility class provides a list of types frequently used by the RPC protocols included in this package.
 * 
 * @author warneke
 */
public class ServerTypeUtils {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private ServerTypeUtils() {
	}

	/**
	 * Returns a list of types frequently used by the RPC protocols of this package and its parent packages.
	 * 
	 * @return a list of types frequently used by the RPC protocols of this package
	 */
	public static List<Class<?>> getRPCTypesToRegister() {

		final List<Class<?>> types = ManagementTypeUtils.getRPCTypesToRegister();

		types.add(AbstractTaskResult.ReturnCode.class);
		types.add(ChannelDeploymentDescriptor.class);
		types.add(CheckpointState.class);
		types.add(ConnectionInfoLookupResponse.class);
		types.add(ConnectionInfoLookupResponse.ReturnCode.class);
		types.add(ExecutionVertexID.class);
		types.add(FileInputSplit.class);
		types.add(GateDeploymentDescriptor.class);
		types.add(HashSet.class);
		types.add(Inet4Address.class);
		types.add(InetSocketAddress.class);
		types.add(InputSplitWrapper.class);
		types.add(InstanceConnectionInfo.class);
		types.add(LocalInstance.class);
		types.add(RemoteReceiver.class);
		types.add(TaskCancelResult.class);
		types.add(TaskCheckpointState.class);
		types.add(TaskDeploymentDescriptor.class);
		types.add(TaskExecutionState.class);
		types.add(TaskSubmissionResult.class);

		return types;
	}
}
