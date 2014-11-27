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

package org.apache.flink.mesos.core;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.mesos.utility.MesosConfiguration;
import org.apache.flink.mesos.utility.MesosConstants;
import org.apache.flink.mesos.utility.MesosUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * The FlinkMesosScheduler gets offers from the Mesos master about resources that are available on the
 * Mesos slaves. According to the MesosConfiguration given via file or command line options the scheduler decides whether
 * to take the offer and launch a Task- or JobManager on it or to refuse the offer.
 *
 * Further information is available at the official page of Apache Mesos:
 * http://mesos.apache.org/documentation/latest/mesos-architecture/
 */
public class FlinkMesosScheduler implements Scheduler {

	/*
	Constants for the resource Strings of Mesos
	 */
	private static final String MESOS_CPU = "cpus";
	private static final String MESOS_MEMORY = "mem";

	private static final Logger LOG = LoggerFactory.getLogger(FlinkMesosScheduler.class);

	private final MesosConfiguration config;
	private final HashMap<Protos.SlaveID, Protos.TaskInfo> taskManagers = new HashMap<Protos.SlaveID, Protos.TaskInfo>();

	private Protos.FrameworkID frameWorkID = null;
	private Protos.Offer jobManagerOffer = null;
	private Protos.TaskInfo jobManager = null;
	private Protos.Offer webFrontendOffer = null;
	private Protos.TaskInfo webFrontend = null;

	public FlinkMesosScheduler(MesosConfiguration config) {
		LOG.debug("Scheduler launched");
		LOG.debug("jar dir: " + config.getString(MesosConstants.MESOS_UBERJAR_LOCATION, null));
		this.config = config;
	}

	private void printOffers(List<Protos.Offer> offers) {
		for (Protos.Offer offer: offers) {
			LOG.debug("-----Got offer from " + offer.getSlaveId().getValue() + "-----");
			for (Protos.Resource resource: offer.getResourcesList()) {
				LOG.debug(resource.getName() + " = " + resource.getScalar().getValue());
			}
		}
	}

	/**
	 * Checks whether the resources that are required are met by the offer.
	 * @param offer The offer that is to be checked.
	 * @param required The required resources.
	 * @return
	 */
	private boolean resourcesMet(Protos.Offer offer, List<Protos.Resource> required) {
		Protos.Resource newcpus = null;
		Protos.Resource oldcpus = null;

		for (Protos.Resource req: required) {
			for (Protos.Resource avail: offer.getResourcesList()) {
				if (avail.getName().equals(req.getName()) && avail.getScalar().getValue() < req.getScalar().getValue()) {
					if (avail.getName().equals("cpus")) {
						LOG.info("Not enough cpus available on " + offer.getHostname() + ". Available: " + avail.getScalar().getValue() + " Maximum: " + req.getScalar().getValue());
						newcpus = avail;
						oldcpus = req;
						continue;
					}
					return false;
				}
			}
		}

		if (oldcpus != null && newcpus != null) {
			required.remove(oldcpus);
			required.add(newcpus);
		}
		return true;
	}

	/**
	 * Helper function that prints infos about the tasks that are launched.
	 * @param name Name of the task.
	 * @param offer The offer of the task.
	 * @param resources The resources the task has.
	 */
	private void logLaunchInfo(String name, Protos.Offer offer, List<Protos.Resource> resources) {
		LOG.info("---- Launching Flink " + name + "----");
		LOG.info("Hostname: " + offer.getHostname());
		LOG.info("SlaveID: " + offer.getSlaveId().getValue());
		for (Protos.Resource resource: resources) {
			LOG.info(resource.getName() + ": " + resource.getScalar().getValue());
		}
	}

	/**
	 * Helper method to create a JobManager TaskInfo on the Mesos slave that made the offer.
	 * @param offer The resource offer from the Mesos slave.
	 * @return TaskInfo that contains the information required to launch a JobManager (command to be executed, memory, cpus, etc.)
	 */
	private Protos.TaskInfo createJobManagerTask(Protos.Offer offer, List<Protos.Resource> resources) {
		double memory = -1.0;
		for (Protos.Resource r: resources) {
			if (r.getName().equals(MESOS_MEMORY)) {
				memory = r.getScalar().getValue();
			}
		}

		try {
			InetAddress address = InetAddress.getByName(offer.getHostname());
			LOG.info("Jobmanager address: " + address.getHostAddress());
			LOG.info("Jobmanager port: " + this.config.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1));
			this.config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, address.getHostAddress());
		} catch (UnknownHostException e) {
			this.config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, offer.getHostname());
		}

		String flinkJMCommand = "java " + "-Xmx" + MesosUtils.calculateMemory(memory) + "M -cp " + config.getString(MesosConstants.MESOS_UBERJAR_LOCATION, ".") + " org.apache.flink.mesos.executors.FlinkJMExecutor";
		Protos.ExecutorInfo jobManagerExecutor = MesosUtils.createExecutorInfo("jm", "Jobmanager Executor", flinkJMCommand, this.config);

		logLaunchInfo("Jobmanager", offer, resources);

		return MesosUtils.createTaskInfo("JobManager", resources, jobManagerExecutor, offer.getSlaveId(), Protos.TaskID.newBuilder().setValue("jm_task-" + jobManagerExecutor.hashCode()).build());
	}

	/**
	 * Helper method to create a TaskManager TaskInfo on the Mesos slave that made the offer.
	 * @param offer The resource offer from the Mesos slave.
	 * @return TaskInfo that contains the information required to launch a TaskManager (command to be executed, memory, cpus, etc.)
	 */
	private Protos.TaskInfo createTaskManagerTask(Protos.Offer offer, List<Protos.Resource> resources) {
		double memory = -1.0;
		for (Protos.Resource r: resources) {
			if (r.getName().equals(MESOS_MEMORY)) {
				memory = r.getScalar().getValue();
			}
		}

		String flinkTMCommand = "java " + "-Xmx" + MesosUtils.calculateMemory(memory) + "M -cp " + config.getString(MesosConstants.MESOS_UBERJAR_LOCATION, ".") + " org.apache.flink.mesos.executors.FlinkTMExecutor";
		Protos.ExecutorInfo taskManagerExecutor = MesosUtils.createExecutorInfo("tm", "Taskmanager Executor", flinkTMCommand, this.config);

		logLaunchInfo("Taskmanager", offer, resources);

		return MesosUtils.createTaskInfo("TaskManager", resources, taskManagerExecutor, offer.getSlaveId(), Protos.TaskID.newBuilder().setValue("tm_task-" + taskManagerExecutor.hashCode()).build());
	}

	@Override
	public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
		LOG.info("Flink was registered: " + frameworkID.getValue() + " " + masterInfo.getHostname());

		if (this.config != null) {
			int port = this.config.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);
			port = MesosUtils.offsetPort(port, frameworkID.hashCode());
			this.config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, port);
		}
	}

	@Override
	public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {

	}


	@Override
	public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {
		Integer maxTaskManagers = config.getInteger(MesosConstants.MESOS_MAX_TM_INSTANCES, Integer.MAX_VALUE);
		printOffers(offers);

		/*
		Tasks contains the tasks that are to be executed and offerIDs the corresponding offer.
		 */
		List<Protos.TaskInfo> tasks = new LinkedList<Protos.TaskInfo>();
		List<Protos.OfferID> offerIDs = new LinkedList<Protos.OfferID>();

		/*
		This loop searches through all the resource offers from the Mesos slaves. If no JobManager is currently
		running it is started. Also, one TaskManager is started on every node that has sufficient resources available.
		 */
		for (Protos.Offer offer : offers) {
			if (jobManager == null && !taskManagers.containsKey(offer.getSlaveId())) {

				List<Protos.Resource> required = new ArrayList<Protos.Resource>();
				required.add(MesosUtils.createResourceScalar(MESOS_CPU, this.config.getDouble(MesosConstants.MESOS_JOB_MANAGER_CORES, MesosConstants.DEFAULT_MESOS_JOB_MANAGER_CORES)));
				required.add(MesosUtils.createResourceScalar(MESOS_MEMORY, this.config.getDouble(MesosConstants.MESOS_JOB_MANAGER_MEMORY, MesosConstants.DEFAULT_MESOS_JOB_MANAGER_MEMORY)));

				if (resourcesMet(offer, required)) {
					Protos.TaskInfo task = createJobManagerTask(offer, required);
					tasks.add(task);
					jobManager = task;
					jobManagerOffer = offer;
					offerIDs.add(offer.getId());
				}
			} else if (!taskManagers.containsKey(offer.getSlaveId()) && taskManagers.size() < maxTaskManagers) {

				List<Protos.Resource> required = new ArrayList<Protos.Resource>();
				required.add(MesosUtils.createResourceScalar(MESOS_CPU, this.config.getDouble(MesosConstants.MESOS_TASK_MANAGER_CORES, MesosConstants.DEFAULT_MESOS_TASK_MANAGER_CORES)));
				required.add(MesosUtils.createResourceScalar(MESOS_MEMORY, this.config.getDouble(MesosConstants.MESOS_TASK_MANAGER_MEMORY, MesosConstants.DEFAULT_MESOS_TASK_MANAGER_MEMORY)));

				if (resourcesMet(offer, required)) {
					Protos.TaskInfo task = createTaskManagerTask(offer, required);
					taskManagers.put(offer.getSlaveId(), task);
					tasks.add(task);
					offerIDs.add(offer.getId());
				}
			}
		}

		if (offerIDs.size() == tasks.size() && offerIDs.size() > 0 && tasks.size() > 0) {
			schedulerDriver.launchTasks(offerIDs, tasks);
		}
	}


	/*
	 * If an offer is no longer available and we tried to deploy a JobManager or TaskManager on it, we try to kill the
	 * affected manager and rerequest the resources that are necessary to start a new one (especially for JobManager as it is required
	 * for Flink to do any job.
	 */
	@Override
	public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
		//TODO: Verify that this mechanism actually works as expected
		LOG.debug("Resource offer with ID: " + offerID.getValue() + " was rescinded.");

		/*
		Check whether the rescinded offer is the one the jobmanager should be started on. If that is the case,
		we immediately kill the task and request enough resources for a new one. These new resources will come in via
		the resourceOffers() method.
		 */
		if (jobManagerOffer != null && jobManager != null && jobManagerOffer.getId().equals(offerID)) {
			LinkedList<Protos.Request> requestList = new LinkedList<Protos.Request>();

			schedulerDriver.killTask(jobManager.getTaskId());
			jobManager = null;
			jobManagerOffer = null;

			LOG.debug("Rescinded offer was jobManager offer, trying to request new resources");
			Protos.Request request = Protos.Request
					.newBuilder()
					.addResources(MesosUtils.createResourceScalar(MESOS_CPU, 1.0))
					.addResources(MesosUtils.createResourceScalar(MESOS_MEMORY, config.getDouble(MesosConstants.MESOS_JOB_MANAGER_MEMORY, MesosConstants.DEFAULT_MESOS_JOB_MANAGER_MEMORY)))
					.build();
			requestList.add(request);

			schedulerDriver.requestResources(requestList);
		}
	}

	/**
	 * Handles status updates from the executors. If TASK_LOST or TASK_FAILED is received from any executor, the task should be killed. In case of a failed
	 * JobManager we try to allocate new resources.
	 * @param schedulerDriver
	 * @param taskStatus
	 */
	@Override
	public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {

		Protos.TaskInfo taskInfo = null;

		if (jobManagerOffer != null) {
			taskInfo = taskStatus.getSlaveId().equals(jobManagerOffer.getSlaveId()) ? jobManager : taskManagers.get(taskStatus.getSlaveId());
		}

		if (taskInfo == null) {
			return;
		}

		LOG.info("Task " + taskInfo.getExecutor().getName() + " is in state: " + taskStatus.getState());

		if (taskStatus.getState() == Protos.TaskState.TASK_LOST || taskStatus.getState() == Protos.TaskState.TASK_FAILED) {
			schedulerDriver.killTask(taskInfo.getTaskId());
			LinkedList<Protos.Request> requestList = new LinkedList<Protos.Request>();

			if (taskInfo.getTaskId().equals(jobManager.getTaskId())) {
				jobManager = null;
				jobManagerOffer = null;

				Protos.Request request = Protos.Request
						.newBuilder()
						.addResources(MesosUtils.createResourceScalar(MESOS_CPU, 1.0))
						.addResources(MesosUtils.createResourceScalar(MESOS_MEMORY, config.getDouble(MesosConstants.MESOS_JOB_MANAGER_MEMORY, MesosConstants.DEFAULT_MESOS_JOB_MANAGER_MEMORY)))
						.build();
				requestList.add(request);

				schedulerDriver.requestResources(requestList);
			} else {
				taskManagers.remove(taskInfo.getSlaveId());
			}
		}

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
