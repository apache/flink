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

import org.apache.flink.util.Preconditions;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ServiceBusException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by jozh on 6/14/2017.
 * Flink eventhub connnector has implemented with same design of flink kafka connector
 */
public class EventhubClientWrapper implements Serializable {
	private static final long serialVersionUID = -5319150387753930840L;
	private static final Logger logger = LoggerFactory.getLogger(EventhubClientWrapper.class);
	private EventHubClient eventHubClient;
	private PartitionReceiver eventhubReceiver;
	private ConnectionStringBuilder connectionString;
	private String consumerGroup;
	private Long receiverEpoch;

	private Duration receiverTimeout;
	private EventhubOffsetType offsetType;
	private String currentOffset;
	private String partitionId;

	private final int minPrefetchCount = 10;
	private int maxPrefetchCount = 999;
	private int maxEventRate = 0;
	private final Long defaultReceiverEpoch = -1L;
	private final String defaultReceiverTimeout = "60000";

	public void createReveiver(Properties eventhubParams, String partitionId)
		throws IllegalArgumentException, URISyntaxException, IOException, ServiceBusException{
		int maxEventRate = Integer.parseInt(eventhubParams.getProperty("eventhubs.maxRate", "10"));
		this.createReveiver(eventhubParams, partitionId, maxEventRate, PartitionReceiver.START_OF_STREAM);
	}

	public void createReveiver(Properties eventhubParams, String partitionId, String offset)
		throws IllegalArgumentException, URISyntaxException, IOException, ServiceBusException{
		int maxEventRate = Integer.parseInt(eventhubParams.getProperty("eventhubs.maxRate", "10"));
		this.createReveiver(eventhubParams, partitionId, maxEventRate, offset);
	}

	public void createReveiver(Properties eventhubParams, String partitionId, int maxEventRate)
		throws IllegalArgumentException, URISyntaxException, IOException, ServiceBusException{
		this.createReveiver(eventhubParams, partitionId, maxEventRate, PartitionReceiver.START_OF_STREAM);
	}

	/*Will not implement a standalone offset store here, will leverage flink state to save the offset of eventhub*/
	public void createReveiver(Properties eventhubParams, String partitionId, int maxEventRate, String offset)
		throws IllegalArgumentException, URISyntaxException, IOException, ServiceBusException{
		if (eventhubParams.containsKey("eventhubs.uri") && eventhubParams.containsKey("eventhubs.namespace")) {
			throw new IllegalArgumentException("Eventhubs URI and namespace cannot both be specified at the same time.");
		}

		if (eventhubParams.containsKey("eventhubs.namespace")){
			this.connectionString = new ConnectionStringBuilder(
				eventhubParams.getProperty("eventhubs.namespace"),
				eventhubParams.getProperty("eventhubs.name"),
				eventhubParams.getProperty("eventhubs.policyname"),
				eventhubParams.getProperty("eventhubs.policykey"));
		}
		else if (eventhubParams.containsKey("eventhubs.uri")){
			this.connectionString = new ConnectionStringBuilder(new URI(
				eventhubParams.getProperty("eventhubs.uri")),
				eventhubParams.getProperty("eventhubs.name"),
				eventhubParams.getProperty("eventhubs.policyname"),
				eventhubParams.getProperty("eventhubs.policykey"));
		}
		else {
			throw new IllegalArgumentException("Either Eventhubs URI or namespace nust be specified.");
		}

		this.partitionId = Preconditions.checkNotNull(partitionId, "partitionId is no valid, cannot be null or empty");
		this.consumerGroup = eventhubParams.getProperty("eventhubs.consumergroup", EventHubClient.DEFAULT_CONSUMER_GROUP_NAME);
		this.receiverEpoch = Long.parseLong(eventhubParams.getProperty("eventhubs.epoch", defaultReceiverEpoch.toString()));
		this.receiverTimeout = Duration.ofMillis(Long.parseLong(eventhubParams.getProperty("eventhubs.receiver.timeout", defaultReceiverTimeout)));
		this.offsetType = EventhubOffsetType.None;
		this.currentOffset = PartitionReceiver.START_OF_STREAM;

		String previousOffset = offset;

		if (previousOffset.compareTo(PartitionReceiver.START_OF_STREAM) != 0 && previousOffset != null) {

			offsetType = EventhubOffsetType.PreviousCheckpoint;
			currentOffset = previousOffset;

		} else if (eventhubParams.containsKey("eventhubs.filter.offset")) {

			offsetType = EventhubOffsetType.InputByteOffset;
			currentOffset = eventhubParams.getProperty("eventhubs.filter.offset");

		} else if (eventhubParams.containsKey("eventhubs.filter.enqueuetime")) {

			offsetType = EventhubOffsetType.InputTimeOffset;
			currentOffset = eventhubParams.getProperty("eventhubs.filter.enqueuetime");
		}

		this.maxEventRate = maxEventRate;

		if (maxEventRate > 0 && maxEventRate < minPrefetchCount) {
			maxPrefetchCount = minPrefetchCount;
		}
		else if (maxEventRate >= minPrefetchCount && maxEventRate < maxPrefetchCount) {
			maxPrefetchCount = maxEventRate + 1;
		}
		else {
			this.maxEventRate = maxPrefetchCount - 1;
		}

		this.createReceiverInternal();
	}

	public Iterable<EventData> receive () throws ExecutionException, InterruptedException {
		return this.eventhubReceiver.receive(maxEventRate).get();
	}

	public void close(){
		logger.info("Close eventhub client on demand of partition {}", this.partitionId);
		if (this.eventhubReceiver != null){
			try {
				this.eventhubReceiver.closeSync();
			}
			catch (ServiceBusException ex){
				logger.error("Close eventhub client of partition {} failed, reason: {}", this.partitionId, ex.getMessage());
			}
		}
	}

	private void createReceiverInternal() throws IOException, ServiceBusException{
		this.eventHubClient = EventHubClient.createFromConnectionStringSync(this.connectionString.toString());

		switch (this.offsetType){
			case None: {
				if (this.receiverEpoch > defaultReceiverEpoch){
					this.eventhubReceiver = this.eventHubClient.createEpochReceiverSync(consumerGroup, partitionId, currentOffset, receiverEpoch);
				}
				else {
					this.eventhubReceiver = this.eventHubClient.createReceiverSync(consumerGroup, partitionId, currentOffset, false);
				}
				break;
			}
			case PreviousCheckpoint: {
				if (this.receiverEpoch > defaultReceiverEpoch){
					this.eventhubReceiver = this.eventHubClient.createEpochReceiverSync(consumerGroup, partitionId, currentOffset, false, receiverEpoch);
				}
				else {
					this.eventhubReceiver = this.eventHubClient.createReceiverSync(consumerGroup, partitionId, currentOffset, false);
				}
				break;
			}
			case InputByteOffset: {
				if (this.receiverEpoch > defaultReceiverEpoch){
					this.eventhubReceiver = this.eventHubClient.createEpochReceiverSync(consumerGroup, partitionId, currentOffset, false, receiverEpoch);
				}
				else {
					this.eventhubReceiver = this.eventHubClient.createReceiverSync(consumerGroup, partitionId, currentOffset, false);
				}
				break;
			}
			case InputTimeOffset: {
				if (this.receiverEpoch > defaultReceiverEpoch){
					this.eventhubReceiver = this.eventHubClient.createEpochReceiverSync(consumerGroup, partitionId, Instant.ofEpochSecond(Long.parseLong(currentOffset)), receiverEpoch);
				}
				else {
					this.eventhubReceiver = this.eventHubClient.createReceiverSync(consumerGroup, partitionId, Instant.ofEpochSecond(Long.parseLong(currentOffset)));
				}
				break;
			}
		}

		this.eventhubReceiver.setPrefetchCount(maxPrefetchCount);
		this.eventhubReceiver.setReceiveTimeout(this.receiverTimeout);
		logger.info("Successfully create eventhub receiver for partition {}, max_event_rate {}, max_prefetch_rate {}, receive_timeout {}, offset {}, ",
			this.partitionId,
			this.maxEventRate,
			this.maxPrefetchCount,
			this.receiverTimeout,
			this.currentOffset);
	}

	public Duration getReceiverTimeout() {
		return receiverTimeout;
	}
}
