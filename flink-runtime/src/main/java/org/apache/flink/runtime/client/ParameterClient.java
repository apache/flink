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

package org.apache.flink.runtime.client;

import akka.pattern.Patterns;
import akka.util.Timeout;
import com.google.common.base.Preconditions;
import org.apache.flink.api.common.server.Parameter;
import org.apache.flink.api.common.server.Update;
import org.apache.flink.api.common.server.UpdateStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.ServerMessages;
import scala.concurrent.Await;
import scala.concurrent.Future;

import static org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;
import static org.apache.flink.runtime.messages.ServerMessages.getClientRegistrationSuccess;

/**
 * The ParameterClient serves as User Client for accessing the ParameterServer
 */
public class ParameterClient {

	/** The parameter server actor gateway*/
	private ActorGateway parameterServerActor;

	/** Integer unique id of this client. This will be managed by the task manager */
	private final int id;

	/**
	 * Constructor for ParameterClient. The creator must provide a unique id to this client.
	 *
	 * @param config User Configuration
	 * @param parameterServerActor Gateway to parameter server actor
	 * @param id Integer id of this client (must be unique for every client)
	 */
	public ParameterClient(Configuration config, ActorGateway parameterServerActor, int id) {
		this.parameterServerActor = Preconditions.checkNotNull(parameterServerActor);
		this.id = id;
	}

	/**
	 * Register a key under the batch update policy.
	 *
	 * @param key Key
	 * @param value Value
	 */
	public void registerBatch(String key, Parameter value) throws Exception{
		register(new ServerMessages.RegisterClient(id, key, value, UpdateStrategy.BATCH, 0));
	}

	/**
	 * Register a key under the asynchronous update policy.
	 *
	 * @param key Key
	 * @param value Value
	 */
	public void registerAsync(String key, Parameter value) throws Exception{
		register(new ServerMessages.RegisterClient(id, key, value, UpdateStrategy.ASYNC, 0));
	}

	/**
	 * Register a key under the stale synchronous update policy.
	 *
	 * @param key Key
	 * @param value Value
	 * @param slack Slack value
	 */
	public void registerSSP(String key, Parameter value, int slack) throws Exception{
		register(new ServerMessages.RegisterClient(id, key, value, UpdateStrategy.SSP, slack));
	}

	public void register(ServerMessages.RegisterClient registrationMessage) throws Exception{
		Object message = new LeaderSessionMessage(parameterServerActor.leaderSessionID(), registrationMessage);
		Future<Object> registerStatus = Patterns.ask(parameterServerActor.actor(), message, new Timeout(AkkaUtils.INF_TIMEOUT()));
		Object answer = Await.result(registerStatus, AkkaUtils.INF_TIMEOUT());
		if(answer.getClass().isAssignableFrom(getClientRegistrationSuccess().getClass())){
			// yay! We're registered.
		} else if(answer instanceof ServerMessages.ClientRegistrationRefuse){
			throw new Exception("Parameter Server refused to register key: " + registrationMessage.key());
		} else{
			throw new Exception("Unexpected message received at ParameterClient");
		}
	}

	/**
	 * Update a parameter at the server under key
	 *
	 * @param key Key
	 * @param update Update
	 */
	public void update(String key, Update update) throws Exception{
		// this method blocks forever until we get confirmation from the server.
		Future<Object> updateStatus = Patterns.ask(
				parameterServerActor.actor(),
				new LeaderSessionMessage(parameterServerActor.leaderSessionID(),new ServerMessages.UpdateParameter(key, update)),
				new Timeout(AkkaUtils.INF_TIMEOUT()));
		Object answer = Await.result(updateStatus, AkkaUtils.INF_TIMEOUT());
		if(answer.getClass().isAssignableFrom(ServerMessages.getUpdateSuccess().getClass())){
			// do nothing
		} else if(answer instanceof ServerMessages.UpdateFailure){
			throw new Exception("Failed to update data on server: " +
					((ServerMessages.UpdateFailure) answer).error());
		} else{
			throw new Exception("Unexpected message received at ParameterClient");
		}
	}

	/**
	 * Pull parameter from the server
	 *
	 * @param key Key
	 * @return The current parameter value at the server.
	 */
	public Parameter pull(String key) throws Exception{
		// we'll likely receive an update soon.
		Future<Object> pushStatus = Patterns.ask(
				parameterServerActor.actor(),
				new LeaderSessionMessage(parameterServerActor.leaderSessionID(), new ServerMessages.PullParameter(key)),
				new Timeout(AkkaUtils.INF_TIMEOUT()));
		Object answer = Await.result(pushStatus, AkkaUtils.INF_TIMEOUT());
		if(answer instanceof ServerMessages.PullSuccess){
			ServerMessages.PullSuccess success = (ServerMessages.PullSuccess) answer;
			return success.value();
		} else if(answer instanceof ServerMessages.PullFailure){
			throw new Exception("Failed to push data to server: " +
					((ServerMessages.PullFailure) answer).error());
		} else{
			throw new Exception("Unexpected message received at ParameterClient");
		}
	}
}
