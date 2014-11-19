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
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Executor class is responsible for executing the actual commands on the Mesos slaves.
 */
public class FlinkJMExecutor implements Executor {
	/**
	 * Thread that is started in launchTask(). It launches the JobManager and its web interface.
	 */
	private class JobManagerThread extends Thread{
		private final ExecutorDriver executorDriver;
		private final Protos.TaskInfo taskInfo ;
		private final String flinkConfDir;
		private final Integer portOffset;
		private final Logger LOG = LoggerFactory.getLogger(JobManagerThread.class);

		public JobManagerThread(ExecutorDriver executorDriver, Protos.TaskInfo taskInfo, String flinkConfDir) {
			this.executorDriver = executorDriver;
			this.taskInfo = taskInfo;
			this.flinkConfDir = flinkConfDir;
			this.portOffset = Math.abs(taskInfo.getExecutor().getFrameworkId().getValue().hashCode());
		}

		@Override
		public void run() {
			LOG.info("Starting JM Thread");
			JobManager jobManager;
			try	{
				String[] args = {"-configDir", flinkConfDir, "-executionMode", "cluster"};
				Configuration conf = GlobalConfiguration.getConfiguration();
				//conf.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "127.0.0.1");
				//conf.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT + (this.portOffset % 100));
				LOG.info("JobManager Port: " + conf.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1));
				GlobalConfiguration.includeConfiguration(conf);
				jobManager = JobManager.initialize(args);
				jobManager.startInfoServer();
			} catch (Exception e) {
				e.printStackTrace();
				MesosUtils.setTaskState(executorDriver, taskInfo.getTaskId(), Protos.TaskState.TASK_FAILED);
				this.interrupt();
			}
		}
	}


	final String flinkConfDir;

	public FlinkJMExecutor(String FLINK_CONF_DIR) {
		this.flinkConfDir = FLINK_CONF_DIR;
	}
	private final Logger LOG = LoggerFactory.getLogger(FlinkJMExecutor.class);

	@Override
	public void registered(ExecutorDriver executorDriver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
		LOG.info("JobManager Executor was registered");
		FlinkProtos.Configuration config  = null;
		try {
			config = FlinkProtos.Configuration.parseFrom(executorInfo.getData());
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		LOG.info("received data from executor: " + config.getValues(0).getKey() + " = " + config.getValues(0).getValue());

	}

	@Override
	public void reregistered(ExecutorDriver executorDriver, Protos.SlaveInfo slaveInfo) {
	}

	@Override
	public void disconnected(ExecutorDriver executorDriver) {
	}

	@Override
	public void launchTask(final ExecutorDriver executorDriver, final Protos.TaskInfo taskInfo) {
		JobManagerThread jobManagerThread = new JobManagerThread(executorDriver, taskInfo, flinkConfDir);
		MesosUtils.setTaskState(executorDriver, taskInfo.getTaskId(), Protos.TaskState.TASK_RUNNING);
		jobManagerThread.run();
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
		MesosExecutorDriver driver = new MesosExecutorDriver(new FlinkJMExecutor(args[0]));
		System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
	}
}
