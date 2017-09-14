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

package org.apache.flink.streaming.connectors.eventhubs.internals;

import org.apache.flink.api.java.tuple.Tuple2;

import com.microsoft.azure.eventhubs.EventData;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by jozh on 5/24/2017.
 * Flink eventhub connnector has implemented with same design of flink kafka connector
 * Cause eventhub client can only access one partition at one time, so here we should have multiple eventhub clients
 * In this worker thread, it will receive event from each partition in round robin mode, any partition failed to retrive
 * events will lead thread exception, and leverage flink HA framework to start from begining again
 */
public class EventhubConsumerThread extends Thread {
	private final Logger logger;
	private final Handover handover;
	private final Properties eventhubProps;
	private final EventhubPartitionState[] subscribedPartitionStates;
	private final Map<EventhubPartitionState, EventhubClientWrapper> clients;
	private volatile boolean running;

	public EventhubConsumerThread(
		Logger logger,
		Handover handover,
		Properties eventhubProps,
		String threadName,
		EventhubPartitionState[] subscribedPartitionStates) throws Exception{

		super(threadName);
		setDaemon(true);

		this.logger = logger;
		this.handover = handover;
		this.eventhubProps = eventhubProps;
		this.subscribedPartitionStates = subscribedPartitionStates;
		this.running = true;

		this.clients = new HashMap<>(this.subscribedPartitionStates.length);
		for (int i = 0; i < this.subscribedPartitionStates.length; i++){
			EventhubClientWrapper client = new EventhubClientWrapper();
			this.clients.put(this.subscribedPartitionStates[i], client);
		}
	}

	public void shutdown(){
		logger.info("Shutdown eventhub consumer thread {} on demand", this.getName());
		running = false;
		handover.wakeupProducer();
	}

	@Override
	public void run() {
		if (!running){
			logger.info("Eventhub consumer thread is set to STOP, thread {} exit", this.getName());
			return;
		}

		try {
			logger.info("Starting create {} eventhub clients on {}", this.subscribedPartitionStates.length, this.getName());
			for (Map.Entry<EventhubPartitionState, EventhubClientWrapper> client : clients.entrySet()){
				EventhubPartitionState state = client.getKey();
				client.getValue().createReveiver(this.eventhubProps, Integer.toString(state.getPartition().getParitionId()), state.getOffset());
			}
		}
		catch (Throwable t){
			logger.error("Create eventhub client of {}, error: {}", this.getName(), t);
			handover.reportError(t);
			clearReceiveClients();
			return;
		}

		try {
			int currentClientIndex = 0;
			while (running){
				EventhubPartitionState partitionState = subscribedPartitionStates[currentClientIndex];
				EventhubClientWrapper client = clients.get(partitionState);
				Iterable<EventData> events = client.receive();
				if (events != null){
					handover.produce(Tuple2.of(partitionState.getPartition(), events));
					logger.debug("Received event from {} on {}", partitionState.getPartition().toString(), this.getName());
				}
				else {
					logger.warn("Receive events from {} timeout, timeout set to {}, thread {}",
						partitionState.getPartition().toString(),
						client.getReceiverTimeout(),
						this.getName());
				}

				currentClientIndex++;
				currentClientIndex = currentClientIndex % subscribedPartitionStates.length;
			}
		}
		catch (Throwable t){
			logger.error("Receving events error, {}", t);
			handover.reportError(t);
		}
		finally {
			logger.info("Exit from eventhub consumer thread, {}", this.getName());
			handover.close();
			clearReceiveClients();
		}

		logger.info("EventhubConsumerThread {} quit", this.getName());
	}

	private void clearReceiveClients(){
		if (clients == null){
			return;
		}

		for (Map.Entry<EventhubPartitionState, EventhubClientWrapper> client : clients.entrySet()){
			try {
				client.getValue().close();
				logger.info("Eventhub client for partition {} closed", client.getKey().getPartition().getParitionId());
			}
			catch (Throwable t){
				logger.warn("Error while close eventhub client for partition {}, error is {}",
					client.getKey().getPartition().getParitionId(),
					t.getMessage());
			}
		}
	}
}
