/**
 *
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
 *
 */

package org.apache.flink.streaming.connectors.twitter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

/**
 * Implementation of {@link SourceFunction} specialized to emit tweets from Twitter.
 * It can connect to Twitter Streaming API, collect tweets and 
 */
public class TwitterSource implements SourceFunction<String> {

	private static final Log LOG = LogFactory.getLog(TwitterSource.class);

	private static final long serialVersionUID = 1L;
	private String authPath;
	private transient BlockingQueue<String> queue;
	private int queueSize = 10000;
	private transient BasicClient client;
	private int waitSec = 5;

	private boolean streaming;
	private int numberOfTweets;

	/**
	 * Create {@link TwitterSource} for streaming
	 * @param authPath
	 * Location of the properties file containing the required authentication information. 
	 */
	public TwitterSource(String authPath) {
		this.authPath = authPath;
		streaming = true;
	}

	/**
	 * Create {@link TwitterSource} to 
	 * collect finite number of tweets
	 * @param authPath
	 * Location of the properties file containing the required authentication information. 
	 * @param numberOfTweets
	 * 
	 */
	public TwitterSource(String authPath, int numberOfTweets) {
		this.authPath = authPath;
		streaming = false;
		this.numberOfTweets = numberOfTweets;
	}

	/**
	 * 
	 */
	@Override
	public void invoke(Collector<String> collector) throws Exception {

		initializeConnection();

		
		if (streaming) {
			collectMessages(collector);
		} else {
			collectMessages(collector, numberOfTweets);
		}

		closeConnection();
	}

	/**
	 * Initialize Hosebird Client to be able to consume Twitter's Streaming API
	 */
	private void initializeConnection() {

		if (LOG.isInfoEnabled()) {
			LOG.info("Initializing Twitter Streaming API connection");
		}

		queue = new LinkedBlockingQueue<String>(queueSize);

		StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
		endpoint.stallWarnings(false);

		Authentication auth = authenticate();

		initializeClient(endpoint, auth);

		if (LOG.isInfoEnabled()) {
			LOG.info("Twitter Streaming API connection established successfully");
		}
	}

	private OAuth1 authenticate() {

		Properties authenticationProperties = loadAuthenticationProperties();

		return new OAuth1(authenticationProperties.getProperty("consumerKey"),
				authenticationProperties.getProperty("consumerSecret"),
				authenticationProperties.getProperty("token"),
				authenticationProperties.getProperty("secret"));
	}

	/**
	 * Reads the given properties file for the authentication data.   
	 * @return
	 * the authentication data.
	 */
	private Properties loadAuthenticationProperties() {
		Properties properties = new Properties();
		try {
			InputStream input = new FileInputStream(authPath);
			properties.load(input);
			input.close();
		} catch (IOException ioe) {
			throw new RuntimeException("Cannot open .properties file: " + authPath,
					ioe);
		}
		return properties;
	}

	private void initializeClient(StatusesSampleEndpoint endpoint,
			Authentication auth) {

		client = new ClientBuilder().name("twitterSourceClient")
				.hosts(Constants.STREAM_HOST).endpoint(endpoint)
				.authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		client.connect();
	}

	/**
	 * Put tweets into collector
	 * @param collector
	 * @param piece
	 */
	protected void collectMessages(Collector<String> collector, int piece) {

		if (LOG.isInfoEnabled()) {
			LOG.info("Collecting tweets");
		}

		for (int i = 0; i < piece; i++) {
			collectOneMessage(collector);
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Collecting tweets finished");
		}
	}

	/**
	 * Put tweets into collector
	 * @param collector
	 * 
	 */
	protected void collectMessages(Collector<String> collector) {

		if (LOG.isInfoEnabled()) {
			LOG.info("Tweet-stream begins");
		}

		while (true) {
			collectOneMessage(collector);
		}
	}

	/**
	 * Put one tweet into the collector.
	 * @param collector
	 */
	protected void collectOneMessage(Collector<String> collector) {
		if (client.isDone()) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Client connection closed unexpectedly: "
						+ client.getExitEvent().getMessage());
			}
		}

		try {
			String msg = queue.poll(waitSec, TimeUnit.SECONDS);
			if (msg != null) {
				collector.collect(msg);
			} else {
				if (LOG.isInfoEnabled()) {
					LOG.info("Did not receive a message in " + waitSec
							+ " seconds");
				}
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("'Waiting for tweet' thread is interrupted", e);
		}

	}

	private void closeConnection() {

		if (LOG.isInfoEnabled()) {
			LOG.info("Initiating connection close");
		}

		client.stop();

		if (LOG.isInfoEnabled()) {
			LOG.info("Connection closed successfully");
		}
	}
}