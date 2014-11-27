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

import org.apache.flink.client.web.WebInterfaceServer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.mesos.utility.MesosConfiguration;
import org.apache.flink.mesos.utility.MesosConstants;
import org.apache.flink.mesos.utility.MesosUtils;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Executor class is responsible for executing the actual commands on the Mesos slaves.
 */
public class FlinkJMExecutor extends FlinkExecutor {
	/**
	 * Thread that is started in launchTask(). It launches the JobManager.
	 */
	private class JobManagerThread extends Thread{
		private final ExecutorDriver executorDriver;
		private final Protos.TaskInfo taskInfo ;
		private final Logger LOG = LoggerFactory.getLogger(JobManagerThread.class);
		private final MesosConfiguration config;

		public JobManagerThread(ExecutorDriver executorDriver, Protos.TaskInfo taskInfo, MesosConfiguration config) {
			this.executorDriver = executorDriver;
			this.taskInfo = taskInfo;
			this.config = config;
		}

		@Override
		public void run() {
			LOG.info("Starting JM Thread");
			JobManager jobManager;

			try	{

				String[] args = {"-executionMode", "cluster"};
				String configDir = config.getString(MesosConstants.MESOS_CONF_DIR, null);

				/*
				No configuration directory is passed to the Jobmanager because the
				initialize() function would reload the the configuration into the
				GlobalConfiguration. Since we want to have flexibility to overwrite some of the configuration details
				in the configDir file, we load the configuration manually.
				 */
				if (configDir != null) {
					GlobalConfiguration.loadConfiguration(configDir);
				}
				GlobalConfiguration.includeConfiguration(this.config);

				jobManager = JobManager.initialize(args);
				jobManager.startInfoServer();

				if (config.getBoolean(MesosConstants.MESOS_USE_WEB, false)) {
					Configuration config = GlobalConfiguration.getConfiguration();

					// add flink base dir to config
					config.setString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, configDir + "/..");

					// get the listening port
					int port = config.getInteger(ConfigConstants.WEB_FRONTEND_PORT_KEY,
							ConfigConstants.DEFAULT_WEBCLIENT_PORT);

					// start the server
					WebInterfaceServer server = new WebInterfaceServer(config, port);
					LOG.info("Starting web frontend server on port " + port + '.');
					server.start();
				}
			} catch (Exception e) {
				e.printStackTrace();
				MesosUtils.setTaskState(executorDriver, taskInfo.getTaskId(), Protos.TaskState.TASK_FAILED);
				this.interrupt();
			}
		}
	}

	private final Logger LOG = LoggerFactory.getLogger(FlinkJMExecutor.class);

	@Override
	public void launchTask(final ExecutorDriver executorDriver, final Protos.TaskInfo taskInfo) {
		JobManagerThread jobManagerThread = new JobManagerThread(executorDriver, taskInfo, this.getConfig());
		MesosUtils.setTaskState(executorDriver, taskInfo.getTaskId(), Protos.TaskState.TASK_RUNNING);
		jobManagerThread.run();
	}

	/*
	The main function is called by the Executor.
	 */
	public static void main(String[] args) throws Exception {
		MesosExecutorDriver driver = new MesosExecutorDriver(new FlinkJMExecutor());
		System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
	}
}
