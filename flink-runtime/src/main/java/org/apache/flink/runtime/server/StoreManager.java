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

import static org.apache.flink.runtime.messages.ServerMessages.ClientRegistrationRefuse;
import static org.apache.flink.runtime.messages.ServerMessages.UpdateFailure;

import java.util.HashMap;

/**
 * Store Manager for the Parameter Server which stores keys and pointers to their key managers.
 */
public class StoreManager {

	private final HashMap<String, KeyManager> store;

	public StoreManager(){
		this.store = new HashMap<>();
	}

	public void registerClient(int id, String key, Parameter value, UpdateStrategy strategy, int slack, ActorRef client) {
		synchronized (this){
			if(store.containsKey(key)){
				KeyManager manager = store.get(key);
				if(manager.getStrategy() == strategy && slack == manager.getSlack()) {
					manager.registerClient(id, client);
					// we don't wanna check the current parameter value.
					// It might be this request is coming from a re-registering client. We just send them the clock.
					// If the clock is zero, they can start with their own parameter. Else, they can pull first.
				} else {
					client.tell(new ClientRegistrationRefuse("Using different update strategies and " +
					"slack values for same key"), ActorRef.noSender());
				}
			} else{
				store.put(key, new KeyManager(id, key, strategy, value, slack, client));
			}
		}
	}

	public Parameter pull(String key) throws Exception{
		if(store.containsKey(key)){
			return store.get(key).getParameter();
		} else{
			throw new Exception("Key: " + key + " hasn't been registered with the Parameter Server");
		}
	}

	public void update(String key, Update update, ActorRef client){
		if(store.containsKey(key)){
			store.get(key).update(update, client);
		} else{
			client.tell(new UpdateFailure("Key: " + key + " has not been registered"), ActorRef.noSender());
		}
	}

	public void shutdown(){
		for(KeyManager manager: store.values()){
			manager.shutdown();
		}
		store.clear();
	}
}
