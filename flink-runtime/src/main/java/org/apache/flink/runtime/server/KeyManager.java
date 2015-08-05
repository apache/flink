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


package org.apache.flink.runtime.server;

import akka.actor.ActorRef;
import org.apache.flink.api.common.server.Parameter;
import org.apache.flink.api.common.server.Update;
import org.apache.flink.api.common.server.UpdateStrategy;

import java.util.ArrayList;
import java.util.HashMap;

import static org.apache.flink.runtime.messages.ServerMessages.getClientRegistrationSuccess;
import static org.apache.flink.runtime.messages.ServerMessages.getUpdateSuccess;
import static org.apache.flink.runtime.messages.ServerMessages.UpdateFailure;

/**
 * Key Manager for the Parameter Server which stores keys and relevant parameter information
 */
public class KeyManager {
	private final String key;
	private final UpdateStrategy strategy;
	private final Parameter parameter;

	// registered client ids
	private ArrayList<Integer> registeredClients;

	// all updates which are pending. Used for BSP and SSP.
	private HashMap<Update, ActorRef> waitingUpdates;

	// clock of the update from the slowest task. This will be useful for SSP.
	// For asynchronous updates, this provides no information.
	// For synchronous updates, all updates are matched with the minimum clock.
	private int minimumClock;

	// slack to be used in SSP iterations
	private final int slack;

	// number of updates received for clock min + 1, min + 2, ..., min + slack.
	private final int[] updateCounts;

	public KeyManager(int id, String key, UpdateStrategy strategy, Parameter parameter, int slack, ActorRef client){
		this.key = key;
		this.strategy = strategy;
		this.parameter = parameter;
		this.registeredClients = new ArrayList<>();
		this.registeredClients.add(id);
		this.waitingUpdates = new HashMap<>();
		// at the first iteration, we would get updates with iteration number 1.
		this.minimumClock = 0;
		this.slack = slack;
		updateCounts = new int[slack - minimumClock + 1];
	}

	public void registerClient(int id, ActorRef client){
		synchronized (this) {
			if (registeredClients.indexOf(id) == -1) {
				registeredClients.add(id);
			}
			// if it was already present, it is a case of re-registration. So we just send an ack.
			client.tell(getClientRegistrationSuccess(), ActorRef.noSender());
		}
	}

	public String getKey(){
		return key;
	}

	public UpdateStrategy getStrategy(){
		return strategy;
	}

	public int getSlack(){
		return slack;
	}

	/**
	 * This will return immediately with whatever current value of parameter we hold.
	 */
	public Parameter getParameter(){
		// let the user know what the next clock value should be.
		parameter.setClock(minimumClock + 1);
		return parameter;
	}

	public void update(Update update, ActorRef client){
		synchronized (this) {
			// there are three cases.
			switch (strategy) {
				case BATCH:
					// we have to wait for everyone to arrive.
					if (update.getClock() != minimumClock + 1) {
						client.tell(new UpdateFailure("Synchronized update strategy requires every task to contribute " +
								"before allowing any task to send a new update."), ActorRef.noSender());
					}
					waitingUpdates.put(update, client);
					// we finalize only when everyone's sent their updates.
					if (waitingUpdates.size() == registeredClients.size()) {
						try {
							ArrayList<Update> updates = new ArrayList<>();
							updates.addAll(waitingUpdates.keySet());
							parameter.reduce(updates);
							minimumClock += 1;
							for(ActorRef gateway: waitingUpdates.values()){
								gateway.tell(getUpdateSuccess(), ActorRef.noSender());
							}
						} catch(Exception e){
							for(ActorRef gateway: waitingUpdates.values()){
								gateway.tell(new UpdateFailure(e.toString()), ActorRef.noSender());
							}
						}
					}
					break;
				case ASYNC:
					// we have no idea of a minimum clock here.
					try {
						parameter.update(update);
						client.tell(getUpdateSuccess(), ActorRef.noSender());
					} catch (Exception e) {
						client.tell(new UpdateFailure(e.toString()), ActorRef.noSender());
					}
					break;
				case SSP:
					// not handled yet.
			}
		}
	}

	public void shutdown(){
		synchronized (this){
			registeredClients.clear();
			waitingUpdates.clear();
		}
	}
}
