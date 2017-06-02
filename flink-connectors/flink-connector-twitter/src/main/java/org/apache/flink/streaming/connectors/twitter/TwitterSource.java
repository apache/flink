/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.twitter;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.common.DelimitedStreamReader;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.core.processor.HosebirdMessageProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;

/**
 * Implementation of {@link SourceFunction} specialized to emit tweets from
 * Twitter. This is not a parallel source because the Twitter API only allows
 * two concurrent connections.
 */
public class TwitterSource extends RichSourceFunction<String> implements StoppableFunction {

	private static final Logger LOG = LoggerFactory.getLogger(TwitterSource.class);

	private static final long serialVersionUID = 1L;

	// ----- Required property keys

	public static final String CONSUMER_KEY = "twitter-source.consumerKey";

	public static final String CONSUMER_SECRET = "twitter-source.consumerSecret";

	public static final String TOKEN = "twitter-source.token";

	public static final String TOKEN_SECRET = "twitter-source.tokenSecret";

	// ------ Optional property keys

	public static final String CLIENT_NAME = "twitter-source.name";

	public static final String CLIENT_HOSTS = "twitter-source.hosts";

	public static final String CLIENT_BUFFER_SIZE = "twitter-source.bufferSize";

	// ----- Fields set by the constructor

	private final Properties properties;

	private EndpointInitializer initializer = new SampleStatusesEndpoint();

	// ----- Runtime fields
	private transient BasicClient client;
	private transient Object waitLock;
	private transient boolean running = true;

	/**
	 * Create {@link TwitterSource} for streaming.
	 *
	 * @param properties For the source
	 */
	public TwitterSource(Properties properties) {
		checkProperty(properties, CONSUMER_KEY);
		checkProperty(properties, CONSUMER_SECRET);
		checkProperty(properties, TOKEN);
		checkProperty(properties, TOKEN_SECRET);

		this.properties = properties;
	}

	private static void checkProperty(Properties p, String key) {
		if (!p.containsKey(key)) {
			throw new IllegalArgumentException("Required property '" + key + "' not set.");
		}
	}

	/**
	 * Set a custom endpoint initializer.
	 */
	public void setCustomEndpointInitializer(EndpointInitializer initializer) {
		Objects.requireNonNull(initializer, "Initializer has to be set");
		ClosureCleaner.ensureSerializable(initializer);
		this.initializer = initializer;
	}

	// ----- Source lifecycle

	@Override
	public void open(Configuration parameters) throws Exception {
		waitLock = new Object();
	}

	@Override
	public void run(final SourceContext<String> ctx) throws Exception {
		LOG.info("Initializing Twitter Streaming API connection");

		StreamingEndpoint endpoint = initializer.createEndpoint();

		Authentication auth = new OAuth1(properties.getProperty(CONSUMER_KEY),
			properties.getProperty(CONSUMER_SECRET),
			properties.getProperty(TOKEN),
			properties.getProperty(TOKEN_SECRET));

		client = new ClientBuilder()
			.name(properties.getProperty(CLIENT_NAME, "flink-twitter-source"))
			.hosts(properties.getProperty(CLIENT_HOSTS, Constants.STREAM_HOST))
			.endpoint(endpoint)
			.authentication(auth)
			.processor(new HosebirdMessageProcessor() {
				public DelimitedStreamReader reader;

				@Override
				public void setup(InputStream input) {
					reader = new DelimitedStreamReader(input, Constants.DEFAULT_CHARSET, Integer.parseInt(properties.getProperty(CLIENT_BUFFER_SIZE, "50000")));
				}

				@Override
				public boolean process() throws IOException, InterruptedException {
					String line = reader.readLine();
					ctx.collect(line);
					return true;
				}
			})
			.build();

		client.connect();
		running = true;

		LOG.info("Twitter Streaming API connection established successfully");

		// just wait now
		while (running) {
			synchronized (waitLock) {
				waitLock.wait(100L);
			}
		}
	}

	@Override
	public void close() {
		this.running = false;
		LOG.info("Closing source");
		if (client != null) {
			// client seems to be thread-safe
			client.stop();
		}
		// leave main method
		synchronized (waitLock) {
			waitLock.notify();
		}
	}

	@Override
	public void cancel() {
		LOG.info("Cancelling Twitter source");
		close();
	}

	@Override
	public void stop() {
		LOG.info("Stopping Twitter source");
		close();
	}

	// ------ Custom endpoints

	/**
	 * Implementing this interface allows users of this source to set a custom endpoint.
	 */
	public interface EndpointInitializer {
		StreamingEndpoint createEndpoint();
	}

	/**
	 * Default endpoint initializer returning the {@see StatusesSampleEndpoint}.
	 */
	private static class SampleStatusesEndpoint implements EndpointInitializer, Serializable {
		@Override
		public StreamingEndpoint createEndpoint() {
			// this default endpoint initializer returns the sample endpoint: Returning a sample from the firehose (all tweets)
			StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
			endpoint.stallWarnings(false);
			endpoint.delimited(false);
			return endpoint;
		}
	}
}
