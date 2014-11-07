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

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class FlinkMesosScheduler implements Scheduler {

	final private String FLINK_JOBMANAGER_COMMAND;
	final private String FLINK_TASKMANAGER_COMMAND;
	final private Protos.ExecutorInfo JOBMANAGER_EXECUTOR;
	final private Protos.ExecutorInfo TASKMANAGER_EXECUTOR;

	private static final Logger LOG = LoggerFactory.getLogger(FlinkMesosScheduler.class);

	private HashMap<Protos.SlaveID, Protos.TaskInfo> taskManagers = new HashMap<Protos.SlaveID, Protos.TaskInfo>();
	private Protos.TaskInfo jobManager = null;
	private Protos.SlaveID jobManagerSlave = null;

	public FlinkMesosScheduler(String uberJarPath, String flinkConfDir) {
		LOG.debug("Scheduler launched");
		LOG.debug("jar dir: " + uberJarPath);
		LOG.debug("conf dir: " + flinkConfDir);

		FLINK_JOBMANAGER_COMMAND = "java -cp " + uberJarPath + " org.apache.flink.mesos.FlinkJMExecutor " + flinkConfDir;
		FLINK_TASKMANAGER_COMMAND = "java -cp " + uberJarPath + " org.apache.flink.mesos.FlinkTMExecutor " + flinkConfDir;
		JOBMANAGER_EXECUTOR = MesosUtils.createExecutorInfo("jm", "JobManager Executor", FLINK_JOBMANAGER_COMMAND);
		TASKMANAGER_EXECUTOR = MesosUtils.createExecutorInfo("tm", "TaskManager Executor", FLINK_TASKMANAGER_COMMAND);
	}

	private void startJobManagerTask(Protos.Offer offer, SchedulerDriver schedulerDriver) {
		LOG.info("---- Launching Flink JobManager----");
		LOG.info("Hostname: " + offer.getHostname());
		LOG.info("SlaveID: " + offer.getSlaveId().getValue());

		List<Protos.TaskInfo> tasks = new LinkedList<Protos.TaskInfo>();
		List<Protos.OfferID> offerIDs = new LinkedList<Protos.OfferID>();

		HashMap<String, Double> resources = new HashMap<String, Double>();
		resources.put("cpus", 1.0);
		resources.put("mem", 512.0);

		LOG.info("JobManager cpus: " + resources.get("cpus"));
		LOG.info("JobManager memory: " + resources.get("mem"));

		Protos.TaskInfo task = MesosUtils.createTaskInfo("JobManager", resources, JOBMANAGER_EXECUTOR, offer.getSlaveId(), Protos.TaskID.newBuilder().setValue("jm_task").build());

		tasks.add(task);
		jobManager = task;
		jobManagerSlave = offer.getSlaveId();

		offerIDs.add(offer.getId());
		schedulerDriver.launchTasks(offerIDs, tasks);
	}

	private void startTaskManager(Protos.Offer offer, SchedulerDriver schedulerDriver) {
		LOG.info("---- Launching TaskManager ----");
		LOG.info("Hostname: " + offer.getHostname());
		LOG.info("SlaveID: " + offer.getSlaveId().getValue());

		List<Protos.TaskInfo> tasks = new LinkedList<Protos.TaskInfo>();
		List<Protos.OfferID> offerIDs = new LinkedList<Protos.OfferID>();



		HashMap<String, Double> resources = new HashMap<String, Double>();
		resources.put("cpus", 1.0);
		resources.put("mem", 512.0);

		LOG.info("TaskManager cpus: " + resources.get("cpus"));
		LOG.info("TaskManager memory: " + resources.get("mem"));

		Protos.TaskInfo task = MesosUtils.createTaskInfo("TaskManager", resources, TASKMANAGER_EXECUTOR, offer.getSlaveId(), Protos.TaskID.newBuilder().setValue("tm_task" + taskManagers.size()).build());

		tasks.add(task);
		taskManagers.put(offer.getSlaveId(), task);

		offerIDs.add(offer.getId());
		schedulerDriver.launchTasks(offerIDs, tasks);
	}

	@Override
	public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
		LOG.info("Flink was registered: " + frameworkID.getValue() + " " + masterInfo.getHostname());
	}

	@Override
	public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {

	}

	@Override
	public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {
		for (Protos.Offer offer : offers) {
			if (jobManager == null && !taskManagers.containsKey(offer.getSlaveId())) {
				startJobManagerTask(offer, schedulerDriver);
			} else if (!taskManagers.containsKey(offer.getSlaveId())) {
				startTaskManager(offer, schedulerDriver);
			}

		}
	}

	@Override
	public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {

	}

	@Override
	public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
		Protos.TaskInfo taskInfo = taskStatus.getSlaveId().equals(jobManagerSlave) ? jobManager : taskManagers.get(taskStatus.getSlaveId());;

		if (taskInfo == null) {
			return;
		}

		LOG.info("Task " + taskInfo.getExecutor().getName() + " is in state: " + taskStatus.getState());
	}

	@Override
	public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {

	}

	@Override
	public void disconnected(SchedulerDriver schedulerDriver) {

	}

	@Override
	public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {
		LOG.info("Lost Connection to slave: " + slaveID.getValue());
	}

	@Override
	public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {
		LOG.info("The following task has exited: " + taskManagers.get(slaveID).getExecutor().getName());
	}

	@Override
	public void error(SchedulerDriver schedulerDriver, String s) {

	}
}
