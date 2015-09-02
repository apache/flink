/*
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

package org.apache.flink.runtime.client.utils;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.messages.JobManagerMessages;
import scala.Option;

import java.util.UUID;

/**
 * A test fake job manager slash cluster which always accepts jobs and handles job status requests
 * according to how it was constructed, the value of parameter mode.
 */
public class FakeTestingCluster{

	private ActorRef jobManager;
	private ActorSystem system;

	public FakeTestingCluster(String mode){
		system = AkkaUtils.createLocalActorSystem(new Configuration());
		jobManager = system.actorOf(Props.create(TestingJobManager.class, mode));
	}

	public ActorGateway getJobManagerGateway() throws Exception{
		return JobManager.getJobManagerGateway(jobManager, AkkaUtils.getTimeout(new Configuration()));
	}

	public void stopCluster(){
		system.shutdown();
	}
}
class TestingJobManager extends FlinkUntypedActor{

	private final String mode;

	private final Option<UUID> leaderSessionID = Option.apply(UUID.randomUUID());

	private int count;

	public TestingJobManager(String mode) {
		this.mode = mode;
		this.count = 0;
	}

	@Override
	protected void handleMessage(Object message) {

		// always accept a job :)
		if(message instanceof JobManagerMessages.SubmitJob){
			sender().tell(decorateMessage(new Status.Success(new JobID())),self());
		}
		// three modes to deal with status request messages
		else if(message instanceof JobManagerMessages.RequestJobStatus){
			// now the mode comes in.
			if(mode.equals("idle")){
				// do nothing. Let the Client actor think we're dead. :')
			} else if(mode.equals("busy")){
				// keep sending out a CREATED message. We're busy. Can't execute your job.
				sender().tell(decorateMessage(new JobManagerMessages.CurrentJobStatus(new JobID(),JobStatus.CREATED)), self());
			} else if(mode.equals("failed")){
				// initially send out one CREATED message, then keep sending out RESTARTING message.
				if(count == 0){
					sender().tell(decorateMessage(new JobManagerMessages.CurrentJobStatus(new JobID(),JobStatus.CREATED)), self());
					count++;
				} else{
					sender().tell(decorateMessage(new JobManagerMessages.CurrentJobStatus(new JobID(),JobStatus.RESTARTING)), self());
				}
			}
		}
		// this has to be done
		else if(message == JobManagerMessages.getRequestLeaderSessionID()){
			sender().tell(new JobManagerMessages.ResponseLeaderSessionID(this.leaderSessionID), self());
		}
	}

	@Override
	protected Option<UUID> getLeaderSessionID(){
		return this.leaderSessionID;
	}
}
