/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.flume;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A Sink for publishing data into Flume.
 * @param <IN>
 */
public class FlumeSink<IN> extends RichSinkFunction<IN> {
	private static final Logger LOG = LoggerFactory.getLogger(FlumeSink.class);

	private static final int DEFAULT_MAXRETRYATTEMPTS = 3;
	private static final long DEFAULT_WAITTIMEMS = 1000L;

	private String clientType;
	private String hostname;
	private int port;
	private int batchSize;
	private int maxRetryAttempts;
	private long waitTimeMs;
	private List<IN> incomingList;
	private FlumeEventBuilder eventBuilder;
	private RpcClient client;

	public FlumeSink(String clientType, String hostname, int port, FlumeEventBuilder<IN> eventBuilder) {
		this(clientType, hostname, port, eventBuilder, RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE, DEFAULT_MAXRETRYATTEMPTS, DEFAULT_WAITTIMEMS);
	}

	public FlumeSink(String clientType, String hostname, int port, FlumeEventBuilder<IN> eventBuilder, int batchSize) {
		this(clientType, hostname, port, eventBuilder, batchSize, DEFAULT_MAXRETRYATTEMPTS, DEFAULT_WAITTIMEMS);
	}

	public FlumeSink(String clientType, String hostname, int port, FlumeEventBuilder<IN> eventBuilder, int batchSize, int maxRetryAttempts, long waitTimeMs) {
		this.clientType = clientType;
		this.hostname = hostname;
		this.port = port;
		this.eventBuilder = eventBuilder;
		this.batchSize = batchSize;
		this.maxRetryAttempts = maxRetryAttempts;
		this.waitTimeMs = waitTimeMs;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		incomingList = new ArrayList();
		client = FlumeUtils.getRpcClient(clientType, hostname, port, batchSize);
	}

	@Override
	public void invoke(IN value) throws Exception {
		int number;
		synchronized (this) {
			if (null != value) {
				incomingList.add(value);
			}
			number = incomingList.size();
		}

		if (number == batchSize) {
			flush();
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		FlumeUtils.destroy(client);
	}

	private void flush() throws IllegalStateException {
		List<Event> events = new ArrayList<>();
		List<IN>  toFlushList;
		synchronized (this) {
			if (incomingList.isEmpty()) {
				return;
			}
			toFlushList = incomingList;
			incomingList = new ArrayList();
		}

		for (IN value: toFlushList) {
			Event event = this.eventBuilder.createFlumeEvent(value, getRuntimeContext());
			events.add(event);
		}

		int retries = 0;
		boolean flag = true;
		while (flag) {
			if (null != client || retries > maxRetryAttempts) {
				flag = false;
			}

			if (retries <= maxRetryAttempts && null == client) {
				LOG.info("Wait for {} ms before retry", waitTimeMs);
				try {
					Thread.sleep(waitTimeMs);
				} catch (InterruptedException ignored) {

				}
				reconnect();
				LOG.info("Retry attempt number {}", retries);
				retries++;
			}
		}

		try {
			client.appendBatch(events);
		} catch (EventDeliveryException e) {
			LOG.info("Encountered exception while sending data to flume : {}", e.getMessage(), e);
		}

	}

	private void reconnect() {
		FlumeUtils.destroy(client);
		client = null;
		client = FlumeUtils.getRpcClient(clientType, hostname, port, batchSize);
	}

}
