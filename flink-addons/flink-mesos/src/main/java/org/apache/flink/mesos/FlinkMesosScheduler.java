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

/**
 * The FlinkMesosScheduler gets offers from the Mesos master about resources that are available on the
 * Mesos slaves. According to the configuration given via file or command line options the scheduler decides whether
 * to take the offer and launch a Task- or JobManager on it or to refuse the offer.
 *
 * Further information is available at the official page of Apache Mesos:
 * http://mesos.apache.org/documentation/latest/mesos-architecture/
 */
public class FlinkMesosScheduler implements Scheduler {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkMesosScheduler.class);
	private MesosConfiguration config;
	private HashMap<Protos.SlaveID, Protos.TaskInfo> taskManagers = new HashMap<Protos.SlaveID, Protos.TaskInfo>();
	private Protos.TaskInfo jobManager = null;
	private Protos.SlaveID jobManagerSlave = null;

	public FlinkMesosScheduler(MesosConfiguration config) {
		LOG.debug("Scheduler launched");
		LOG.debug("jar dir: " + config.get(MesosConfiguration.ConfKeys.FLINK_JAR));
		LOG.debug("conf dir: " + config.get(MesosConfiguration.ConfKeys.FLINK_CONF_DIR));

		this.config = config;

	}

	/**
	 * This method checks whether the resource requirements for the TaskManger are met with the given offer.
	 * @param offer The offer to be checked.
	 * @return True if sufficient resources are available, false otherwise.
	 */
	private boolean resourcesMetTM(Protos.Offer offer) {
		Double cpus;
		Double memory;
		try {
			if (config.get(MesosConfiguration.ConfKeys.TM_CORES) != null ) {
				cpus = new Double(config.get(MesosConfiguration.ConfKeys.TM_CORES));
			} else {
				cpus = new Double(1.0);
				config.set(MesosConfiguration.ConfKeys.TM_CORES, cpus.toString());
			}
		} catch (NumberFormatException nfe) {
			cpus = new Double(1.0);
			config.set(MesosConfiguration.ConfKeys.TM_CORES, cpus.toString());
		}

		try {
			if (config.get(MesosConfiguration.ConfKeys.TM_MEMORY) != null ) {
				memory = new Double(config.get(MesosConfiguration.ConfKeys.TM_MEMORY));
			} else {
				memory = new Double(512.0);
				config.set(MesosConfiguration.ConfKeys.TM_MEMORY, memory.toString());
			}
		} catch (NumberFormatException nfe) {
			memory = new Double(512.0);
			config.set(MesosConfiguration.ConfKeys.TM_MEMORY, memory.toString());
		}

		for (Protos.Resource resource: offer.getResourcesList()) {
			if (resource.getName().equals("cpus") && resource.getScalar().getValue() < cpus) {
				LOG.info("Offer from " + offer.getHostname() + " for TaskManager was rejected:\nCPUs available: " + resource.getScalar().getValue() + " CPUs required: " + cpus);
				return false;
			}
			if (resource.getName().equals("mem") && resource.getScalar().getValue() < memory) {
				LOG.info("Offer from " + offer.getHostname() + " for TaskManager was rejected:\nmemory available: " + resource.getScalar().getValue() + " memory required: " + memory);
				return false;
			}
		}
		return true;
	}

	/**
	 * This method checks whether the resource requirements for the JobManager are met with the given offer.
	 * @param offer The offer to be checked.
	 * @return True if sufficient resources are available, false otherwise.
	 */
	private boolean resourcesMetJM(Protos.Offer offer) {
		Double memory;
		try {
			if (config.get(MesosConfiguration.ConfKeys.JM_MEMORY) != null) {
				memory = new Double(config.get(MesosConfiguration.ConfKeys.JM_MEMORY));
			} else {
				memory = new Double(512.0);
				config.set(MesosConfiguration.ConfKeys.JM_MEMORY, memory.toString());
			}
		} catch (NumberFormatException nfe) {
			memory = new Double(512.0);
			config.set(MesosConfiguration.ConfKeys.JM_MEMORY, memory.toString());
		}

		for (Protos.Resource resource: offer.getResourcesList()) {
			if (resource.getName().equals("mem") && resource.getScalar().getValue() < memory) {
				LOG.info("Offer from " + offer.getHostname() + " for JobManager was rejected:\nmemory available: " + resource.getScalar().getValue() + " memory required: " + memory);
				return false;
			}
		}
		return true;
	}

	/**
	 * Helper method to create a JobManager TaskInfo on the Mesos slave that made the offer.
	 * @param offer The resource offer from the Mesos slave.
	 * @return TaskInfo that contains the information required to launch a JobManager (command to be executed, memory, cpus, etc.)
	 */
	private Protos.TaskInfo createJobManagerTask(Protos.Offer offer) {
		LOG.info("---- Launching Flink JobManager----");
		LOG.info("Hostname: " + offer.getHostname());
		LOG.info("SlaveID: " + offer.getSlaveId().getValue());

		HashMap<String, Double> resources = new HashMap<String, Double>();
		resources.put("cpus", 1.0);
		resources.put("mem", new Double(config.get(MesosConfiguration.ConfKeys.JM_MEMORY)));

		String flinkJMCommand = "java " + "-Xmx" + (int) Double.parseDouble(resources.get("mem").toString()) + "M -cp " + config.get(MesosConfiguration.ConfKeys.FLINK_JAR) + " org.apache.flink.mesos.FlinkJMExecutor " + config.get(MesosConfiguration.ConfKeys.FLINK_CONF_DIR);
		Protos.ExecutorInfo jobManagerExecutor = MesosUtils.createExecutorInfo("jm", "JobManager Executor", flinkJMCommand);

		LOG.info("JobManager cpus: " + resources.get("cpus"));
		LOG.info("JobManager memory: " + resources.get("mem"));

		return MesosUtils.createTaskInfo("JobManager", resources, jobManagerExecutor, offer.getSlaveId(), Protos.TaskID.newBuilder().setValue("jm_task").build());
	}

	/**
	 * Helper method to create a TaskManager TaskInfo on the Mesos slave that made the offer.
	 * @param offer The resource offer from the Mesos slave.
	 * @return TaskInfo that contains the information required to launch a TaskManager (command to be executed, memory, cpus, etc.)
	 */
	private Protos.TaskInfo createTaskManager(Protos.Offer offer) {
		LOG.info("---- Launching TaskManager ----");
		LOG.info("Hostname: " + offer.getHostname());
		LOG.info("SlaveID: " + offer.getSlaveId().getValue());


		HashMap<String, Double> resources = new HashMap<String, Double>();
		resources.put("cpus", new Double(config.get(MesosConfiguration.ConfKeys.TM_CORES)));
		resources.put("mem", new Double(config.get(MesosConfiguration.ConfKeys.TM_MEMORY)));

		String flinkTMCommand = "java " + "-Xmx" + (int) Double.parseDouble(resources.get("mem").toString()) + "M -cp " + config.get(MesosConfiguration.ConfKeys.FLINK_JAR) + " org.apache.flink.mesos.FlinkTMExecutor " + config.get(MesosConfiguration.ConfKeys.FLINK_CONF_DIR);
		Protos.ExecutorInfo taskManagerExecutor = MesosUtils.createExecutorInfo("tm", "TaskManager Executor", flinkTMCommand);

		LOG.info("TaskManager cpus: " + resources.get("cpus"));
		LOG.info("TaskManager memory: " + resources.get("mem"));

		return MesosUtils.createTaskInfo("TaskManager", resources, taskManagerExecutor, offer.getSlaveId(), Protos.TaskID.newBuilder().setValue("tm_task" + taskManagerExecutor.hashCode()).build());
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
		Integer maxTaskManagers;
		/*
		Tasks contains the tasks that are to be executed and offerIDs the corresponding offer.
		 */
		List<Protos.TaskInfo> tasks = new LinkedList<Protos.TaskInfo>();
		List<Protos.OfferID> offerIDs = new LinkedList<Protos.OfferID>();

		if (config.get(MesosConfiguration.ConfKeys.MAX_TM_INSTANCES) != null) {
			maxTaskManagers = new Integer(config.get(MesosConfiguration.ConfKeys.MAX_TM_INSTANCES));
		} else {
			//if there is no limit of taskManagers, a greedy approach is taken and free resources are used.
			maxTaskManagers = Integer.MAX_VALUE;
		}

		/*
		This loop searches through all the resource offers from the Mesos slaves. If no JobManager is currently
		running it is started. Also, one TaskManager is started on every node that has sufficient resources available.
		 */
		for (Protos.Offer offer : offers) {
			if (jobManager == null && !taskManagers.containsKey(offer.getSlaveId())) {
				if (resourcesMetJM(offer)) {
					Protos.TaskInfo task = createJobManagerTask(offer);
					tasks.add(task);
					jobManager = task;
					jobManagerSlave = offer.getSlaveId();
					offerIDs.add(offer.getId());
				}
			} else if (!taskManagers.containsKey(offer.getSlaveId()) && taskManagers.size() < maxTaskManagers) {
				if (resourcesMetTM(offer)) {
					Protos.TaskInfo task = createTaskManager(offer);
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
