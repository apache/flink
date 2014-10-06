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

package org.apache.flink.runtime.minicluster;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.pattern.Patterns;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.util.ArrayList;
import java.util.List;

abstract public class FlinkMiniCluster {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkMiniCluster.class);
	protected static final String HOSTNAME = "localhost";

	protected ActorSystem jobManagerActorSystem = null;
	protected ActorRef jobManagerActor = null;

	protected List<ActorSystem> taskManagerActorSystems = new ArrayList<ActorSystem>();
	protected List<ActorRef> taskManagerActors = new ArrayList<ActorRef>();

	protected abstract Configuration generateConfiguration(final Configuration userConfiguration);

	public abstract ActorRef startJobManager(final ActorSystem system, final Configuration configuration);
	public abstract ActorRef startTaskManager(final ActorSystem system, final Configuration configuration,
											final int index);

	ActorSystem startJobManagerActorSystem(final Configuration configuration) {
		int port = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

		return AkkaUtils.createActorSystem(HOSTNAME, port, configuration);
	}

	ActorSystem startTaskManagerActorSystem(final Configuration configuration, int index){
		int port = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY,
				ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT);

		if(port != 0){
			port += index;
		}

		return AkkaUtils.createActorSystem(HOSTNAME, port, configuration);
	}

	public ActorRef getJobManager() {
		return jobManagerActor;
	}

	public List<ActorRef> getTaskManagers() {
		return taskManagerActors;
	}

	// ------------------------------------------------------------------------
	// Life cycle and Job Submission
	// ------------------------------------------------------------------------


	public void start(final Configuration configuration) {

		Configuration clusterConfiguration = generateConfiguration(configuration);

		jobManagerActorSystem = startJobManagerActorSystem(clusterConfiguration);
		jobManagerActor = startJobManager(jobManagerActorSystem, clusterConfiguration);

		int numTaskManagers = clusterConfiguration.getInteger(ConfigConstants
				.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 1);

		for(int i = 0; i < numTaskManagers; i++){
			ActorSystem actorSystem = startTaskManagerActorSystem(clusterConfiguration, i);
			ActorRef taskManager = startTaskManager(actorSystem, clusterConfiguration, i);
			taskManagerActorSystems.add(actorSystem);
			taskManagerActors.add(taskManager);
		}

		waitForTaskManagersToBeRegistered();
	}

	public void stop() {
		LOG.info("Stopping FlinkMiniCluster.");
		shutdown();

		awaitTermination();

		taskManagerActorSystems.clear();
		taskManagerActors.clear();
	}

	protected void shutdown() {
		for(ActorSystem system: taskManagerActorSystems){
			system.shutdown();
		}
		jobManagerActorSystem.shutdown();
	}

	protected void awaitTermination() {
		for(ActorSystem system: taskManagerActorSystems){
			system.awaitTermination(AkkaUtils.AWAIT_DURATION());
		}
		jobManagerActorSystem.awaitTermination(AkkaUtils.AWAIT_DURATION());
	}



	// ------------------------------------------------------------------------
	// Network utility methods
	// ------------------------------------------------------------------------

	private void waitForTaskManagersToBeRegistered(){
		List<Future<Object>> responses = new ArrayList<Future<Object>>();

		for(ActorRef taskManager: taskManagerActors){
			Future<Object> response = Patterns.ask(taskManager, TaskManagerMessages.NotifyWhenRegisteredAtMaster$
					.MODULE$, AkkaUtils.FUTURE_TIMEOUT());
			responses.add(response);
		}

		try {
			Await.ready(Futures.sequence(responses, AkkaUtils.globalExecutionContext()), AkkaUtils.AWAIT_DURATION());
		}catch(Exception e){
			throw new RuntimeException("Not all task managers could register at the job manager.", e);
		}
	}
}
