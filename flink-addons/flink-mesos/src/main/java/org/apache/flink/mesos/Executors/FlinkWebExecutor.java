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
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlinkWebExecutor extends FlinkExecutor {
	private class WebThread extends Thread{
		private final ExecutorDriver executorDriver;
		private final Protos.TaskInfo taskInfo ;
		private final Logger LOG = LoggerFactory.getLogger(FlinkWebExecutor.class);
		private final MesosConfiguration config;

		public WebThread(ExecutorDriver executorDriver, Protos.TaskInfo taskInfo, MesosConfiguration config) {
			this.executorDriver = executorDriver;
			this.taskInfo = taskInfo;
			this.config = config;
		}

		@Override
		public void run() {
			LOG.info("Running WebFrontend Thread");

			// First, try to load global configuration
			String confDir = config.getString(MesosConstants.MESOS_CONF_DIR, null);

			// Create a new task manager object
			try {
				// load the global configuration
				GlobalConfiguration.loadConfiguration(confDir);
				Configuration config = GlobalConfiguration.getConfiguration();

				// add flink base dir to config
				config.setString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, confDir+"/..");

				// get the listening port
				int port = config.getInteger(ConfigConstants.WEB_FRONTEND_PORT_KEY,
						ConfigConstants.DEFAULT_WEBCLIENT_PORT);

				// start the server
				WebInterfaceServer server = new WebInterfaceServer(config, port);
				LOG.info("Starting web frontend server on port " + port + '.');
				server.start();
				//server.join();
			} catch (Throwable t) {
				MesosUtils.setTaskState(executorDriver, taskInfo.getTaskId(), Protos.TaskState.TASK_FAILED);
			}
		}

	}

	private static final Logger LOG = LoggerFactory.getLogger(FlinkWebExecutor.class);


	@Override
	public void launchTask(final ExecutorDriver executorDriver, final Protos.TaskInfo taskInfo) {
		WebThread webThread = new WebThread(executorDriver, taskInfo, this.getConfig());
		MesosUtils.setTaskState(executorDriver, taskInfo.getTaskId(), Protos.TaskState.TASK_RUNNING);
		webThread.run();
	}


	public static void main(String[] args) throws Exception {
		MesosExecutorDriver driver = new MesosExecutorDriver(new FlinkWebExecutor());
		System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
	}
}
