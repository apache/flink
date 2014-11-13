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

import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;

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

		public JobManagerThread(ExecutorDriver executorDriver, Protos.TaskInfo taskInfo, String flinkConfDir) {
			this.executorDriver = executorDriver;
			this.taskInfo = taskInfo;
			this.flinkConfDir = flinkConfDir;
		}

		@Override
		public void run() {
			System.out.println("Starting JM thread");
			JobManager jobManager;
			try	{
				String[] args = {"-configDir", flinkConfDir, "-executionMode", "cluster"};
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

	@Override
	public void registered(ExecutorDriver executorDriver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
		System.out.println("JobManager Executor was registered");
		System.out.println("yolo");
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
