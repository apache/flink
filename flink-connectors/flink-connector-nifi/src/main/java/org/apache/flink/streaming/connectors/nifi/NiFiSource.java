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

package org.apache.flink.streaming.connectors.nifi;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A source that pulls data from Apache NiFi using the NiFi Site-to-Site client. This source
 * produces NiFiDataPackets which encapsulate the content and attributes of a NiFi FlowFile.
 */
public class NiFiSource extends RichParallelSourceFunction<NiFiDataPacket> implements StoppableFunction{

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(NiFiSource.class);

	private static final long DEFAULT_WAIT_TIME_MS = 1000;

	// ------------------------------------------------------------------------

	private final SiteToSiteClientConfig clientConfig;

	private final long waitTimeMs;

	private transient SiteToSiteClient client;

	private volatile boolean isRunning = true;

	/**
	 * Constructs a new NiFiSource using the given client config and the default wait time of 1000 ms.
	 *
	 * @param clientConfig the configuration for building a NiFi SiteToSiteClient
	 */
	public NiFiSource(SiteToSiteClientConfig clientConfig) {
		this(clientConfig, DEFAULT_WAIT_TIME_MS);
	}

	/**
	 * Constructs a new NiFiSource using the given client config and wait time.
	 *
	 * @param clientConfig the configuration for building a NiFi SiteToSiteClient
	 * @param waitTimeMs the amount of time to wait (in milliseconds) if no data is available to pull from NiFi
	 */
	public NiFiSource(SiteToSiteClientConfig clientConfig, long waitTimeMs) {
		this.clientConfig = clientConfig;
		this.waitTimeMs = waitTimeMs;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		client = new SiteToSiteClient.Builder().fromConfig(clientConfig).build();
	}

	@Override
	public void run(SourceContext<NiFiDataPacket> ctx) throws Exception {
		while (isRunning) {
			final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);
			if (transaction == null) {
				LOG.warn("A transaction could not be created, waiting and will try again...");
				try {
					Thread.sleep(waitTimeMs);
				} catch (InterruptedException ignored) {

				}
				continue;
			}

			DataPacket dataPacket = transaction.receive();
			if (dataPacket == null) {
				transaction.confirm();
				transaction.complete();

				LOG.debug("No data available to pull, waiting and will try again...");
				try {
					Thread.sleep(waitTimeMs);
				} catch (InterruptedException ignored) {

				}
				continue;
			}

			final List<NiFiDataPacket> niFiDataPackets = new ArrayList<>();
			do {
				// Read the data into a byte array and wrap it along with the attributes
				// into a NiFiDataPacket.
				final InputStream inStream = dataPacket.getData();
				final byte[] data = new byte[(int) dataPacket.getSize()];
				StreamUtils.fillBuffer(inStream, data);

				final Map<String, String> attributes = dataPacket.getAttributes();

				niFiDataPackets.add(new StandardNiFiDataPacket(data, attributes));
				dataPacket = transaction.receive();
			} while (dataPacket != null);

			// Confirm transaction to verify the data
			transaction.confirm();

			for (NiFiDataPacket dp : niFiDataPackets) {
				ctx.collect(dp);
			}

			transaction.complete();
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public void close() throws Exception {
		super.close();
		client.close();
	}

	@Override
	public void stop() {
		this.isRunning = false;
	}
}
