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

package org.apache.flink.mesos.executors;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.mesos.utility.FlinkProtos;
import org.apache.flink.mesos.utility.MesosConfiguration;
import org.apache.flink.mesos.utility.MesosUtils;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for any Flink Executors. It contains basic functionality such as retrieving the configuration
 * from the ExecutorInfo and printing Log Information to the user.
 */
public abstract class FlinkExecutor implements Executor {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkExecutor.class);
	private MesosConfiguration config = new MesosConfiguration();
	private String executorName;

	@Override
	public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
		this.executorName = executorInfo.getName();

		LOG.info(executorInfo.getName() + " was registered on host " + slaveInfo.getHostname());

		/*
		Retrieve the config that was serialized as Google ProtoBuf and passed to the ExecutorInfo
		 */
		FlinkProtos.Configuration protoConfig  = null;

		try {
			protoConfig = FlinkProtos.Configuration.parseFrom(executorInfo.getData());
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		config.fromProtos(protoConfig);
		LOG.info(config.toString());
	}

	@Override
	public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
		LOG.info(executorName + " reregistered with Slave " + slaveInfo.getHostname());
	}

	@Override
	public void disconnected(ExecutorDriver driver) {
		LOG.info(executorName + " was disconnected from Mesos Slave.");
	}

	@Override
	public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
		MesosUtils.setTaskState(driver, taskId, Protos.TaskState.TASK_KILLED);
	}

	@Override
	public void frameworkMessage(ExecutorDriver driver, byte[] data) {

	}

	@Override
	public void shutdown(ExecutorDriver driver) {

	}

	@Override
	public void error(ExecutorDriver driver, String message) {

	}

	protected MesosConfiguration getConfig() {
		return this.config;
	}

	protected void setConfig(MesosConfiguration config) {
		this.config = config;
	}
}
