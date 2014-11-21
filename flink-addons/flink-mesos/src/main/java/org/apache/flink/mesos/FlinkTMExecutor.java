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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.ExecutionMode;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlinkTMExecutor implements Executor {
	private class TaskManagerThread extends Thread{
		private final ExecutorDriver executorDriver;
		private final Protos.TaskInfo taskInfo ;
		private final int portOffset;
		private final Logger LOG = LoggerFactory.getLogger(FlinkTMExecutor.class);
		private final MesosConfiguration config;

		public TaskManagerThread(ExecutorDriver executorDriver, Protos.TaskInfo taskInfo, MesosConfiguration config) {
			this.executorDriver = executorDriver;
			this.taskInfo = taskInfo;
			this.config = config;
			this.portOffset = Math.abs(taskInfo.getExecutor().getFrameworkId().getValue().hashCode());
		}

		@Override
		public void run() {
			LOG.info("Running TaskManager Thread");
			double cpus = 1.0;

			for (Protos.Resource resource: taskInfo.getResourcesList()){
				if (resource.getName().equals("cpus")) {
					cpus = resource.getScalar().getValue();
				}
			}

			// First, try to load global configuration
			GlobalConfiguration.loadConfiguration(config.getString(MesosConstants.MESOS_CONF_DIR, "."));
			Configuration conf = GlobalConfiguration.getConfiguration();
			conf.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, config.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, (int) cpus));
			LOG.info("JobManager Port: " + conf.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1));
			GlobalConfiguration.includeConfiguration(conf);
			// Create a new task manager object
			try {
				TaskManager.createTaskManager(ExecutionMode.CLUSTER);
			} catch (Throwable t) {
				MesosUtils.setTaskState(executorDriver, taskInfo.getTaskId(), Protos.TaskState.TASK_FAILED);
			}
		}

	}

	private static final Logger LOG = LoggerFactory.getLogger(FlinkTMExecutor.class);
	private MesosConfiguration config = new MesosConfiguration();

	@Override
	public void registered(ExecutorDriver executorDriver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
		LOG.info("TaskManager Executor was registered on host " + slaveInfo.getHostname());

		FlinkProtos.Configuration protoConfig  = null;

		try {
			protoConfig = FlinkProtos.Configuration.parseFrom(executorInfo.getData());
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		config.fromProtos(protoConfig);
	}

	@Override
	public void reregistered(ExecutorDriver executorDriver, Protos.SlaveInfo slaveInfo) {
	}

	@Override
	public void disconnected(ExecutorDriver executorDriver) {
	}

	@Override
	public void launchTask(final ExecutorDriver executorDriver, final Protos.TaskInfo taskInfo) {
		TaskManagerThread taskManagerThread = new TaskManagerThread(executorDriver, taskInfo, this.config);
		MesosUtils.setTaskState(executorDriver, taskInfo.getTaskId(), Protos.TaskState.TASK_RUNNING);
		taskManagerThread.run();
	}

	@Override
	public void killTask(ExecutorDriver executorDriver, Protos.TaskID taskID) {
		MesosUtils.setTaskState(executorDriver, taskID, Protos.TaskState.TASK_KILLED);
	}

	@Override
	public void frameworkMessage(ExecutorDriver executorDriver, byte[] bytes) {

	}

	@Override
	public void shutdown(ExecutorDriver executorDriver) {

	}

	@Override
	public void error(ExecutorDriver executorDriver, String s) {
	}

	public static void main(String[] args) throws Exception {
		MesosExecutorDriver driver = new MesosExecutorDriver(new FlinkTMExecutor());
		System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
	}
}
