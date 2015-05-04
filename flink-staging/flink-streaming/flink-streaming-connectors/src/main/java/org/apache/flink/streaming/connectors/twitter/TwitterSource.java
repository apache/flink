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

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

/**
 * Implementation of {@link SourceFunction} specialized to emit tweets from
 * Twitter. It can connect to Twitter Streaming API, collect tweets and
 */
public class TwitterSource extends RichParallelSourceFunction<String> {

	private static final Logger LOG = LoggerFactory.getLogger(TwitterSource.class);

	private static final long serialVersionUID = 1L;
	private String authPath;
	private transient BlockingQueue<String> queue;
	private int queueSize = 10000;
	private transient BasicClient client;
	private int waitSec = 5;

	private int maxNumberOfTweets;
	private int currentNumberOfTweets;

	private String nextElement = null;

	private volatile boolean isRunning = false;

	/**
	 * Create {@link TwitterSource} for streaming
	 * 
	 * @param authPath
	 *            Location of the properties file containing the required
	 *            authentication information.
	 */
	public TwitterSource(String authPath) {
		this.authPath = authPath;
		maxNumberOfTweets = -1;
	}

	/**
	 * Create {@link TwitterSource} to collect finite number of tweets
	 * 
	 * @param authPath
	 *            Location of the properties file containing the required
	 *            authentication information.
	 * @param numberOfTweets
	 * 
	 */
	public TwitterSource(String authPath, int numberOfTweets) {
		this.authPath = authPath;
		this.maxNumberOfTweets = numberOfTweets;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		initializeConnection();
		currentNumberOfTweets = 0;
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
	 * 
	 * @return the authentication data.
	 */
	private Properties loadAuthenticationProperties() {
		Properties properties = new Properties();
		try {
			InputStream input = new FileInputStream(authPath);
			properties.load(input);
			input.close();
		} catch (Exception e) {
			throw new RuntimeException("Cannot open .properties file: " + authPath, e);
		}
		return properties;
	}

	private void initializeClient(StatusesSampleEndpoint endpoint, Authentication auth) {

		client = new ClientBuilder().name("twitterSourceClient").hosts(Constants.STREAM_HOST)
				.endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		client.connect();
	}

	/**
	 * Put tweets into output
	 * 
	 * @param collector
	 *            Collector in which the tweets are collected.
	 */
	protected void collectFiniteMessages(Collector<String> collector) {

		if (LOG.isInfoEnabled()) {
			LOG.info("Collecting tweets");
		}

		for (int i = 0; i < maxNumberOfTweets; i++) {
			collectOneMessage(collector);
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Collecting tweets finished");
		}
	}

	/**
	 * Put tweets into output
	 * 
	 * @param collector
	 *            Collector in which the tweets are collected.
	 */
	protected void collectMessages(Collector<String> collector) {

		if (LOG.isInfoEnabled()) {
			LOG.info("Tweet-stream begins");
		}

		while (isRunning) {
			collectOneMessage(collector);
		}
	}

	/**
	 * Put one tweet into the output.
	 * 
	 * @param collector
	 *            Collector in which the tweets are collected.
	 */
	protected void collectOneMessage(Collector<String> collector) {
		if (client.isDone()) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Client connection closed unexpectedly: {}", client.getExitEvent()
						.getMessage());
			}
		}

		try {
			String msg = queue.poll(waitSec, TimeUnit.SECONDS);
			if (msg != null) {
				collector.collect(msg);
			} else {
				if (LOG.isInfoEnabled()) {
					LOG.info("Did not receive a message in {} seconds", waitSec);
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

	/**
	 * Get the size of the queue in which the tweets are contained temporarily.
	 * 
	 * @return the size of the queue in which the tweets are contained
	 *         temporarily
	 */
	public int getQueueSize() {
		return queueSize;
	}

	/**
	 * Set the size of the queue in which the tweets are contained temporarily.
	 * 
	 * @param queueSize
	 *            The desired value.
	 */
	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	/**
	 * This function tells how long TwitterSource waits for the tweets.
	 * 
	 * @return Number of second.
	 */
	public int getWaitSec() {
		return waitSec;
	}

	/**
	 * This function sets how long TwitterSource should wait for the tweets.
	 * 
	 * @param waitSec
	 *            The desired value.
	 */
	public void setWaitSec(int waitSec) {
		this.waitSec = waitSec;
	}

	@Override
	public boolean reachedEnd() throws Exception {
		if (currentNumberOfTweets >= maxNumberOfTweets) {
			return false;
		}

		if (nextElement != null) {
			return true;
		}
		if (client.isDone()) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Client connection closed unexpectedly: {}", client.getExitEvent()
						.getMessage());
			}
			return false;
		}

		try {
			String msg = queue.poll(waitSec, TimeUnit.SECONDS);
			if (msg != null) {
				nextElement = msg;
				return true;
			} else {
				if (LOG.isInfoEnabled()) {
					LOG.info("Did not receive a message in {} seconds", waitSec);
				}
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("'Waiting for tweet' thread is interrupted", e);
		}
		return false;
	}

	@Override
	public String next() throws Exception {
		if (nextElement != null) {
			String result = nextElement;
			nextElement = null;
			return result;
		}
		if (reachedEnd()) {
			throw new RuntimeException("Twitter stream end reached.");
		} else {
			String result = nextElement;
			nextElement = null;
			return result;
		}
	}
}
