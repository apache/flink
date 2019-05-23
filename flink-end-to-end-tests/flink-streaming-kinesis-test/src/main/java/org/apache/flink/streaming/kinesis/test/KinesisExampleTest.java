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

package org.apache.flink.streaming.kinesis.test;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.utils.ParameterTool;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test driver for {@link KinesisExample#main}.
 */
public class KinesisExampleTest {
	private static final Logger LOG = LoggerFactory.getLogger(KinesisExampleTest.class);

	/**
	 * Interface to the pubsub system for this test.
	 */
	interface PubsubClient {
		void createTopic(String topic, int partitions, Properties props) throws Exception;

		void sendMessage(String topic, String msg);

		List<String> readAllMessages(String streamName) throws Exception;
	}

	public static void main(String[] args) throws Exception {
		LOG.info("System properties: {}", System.getProperties());
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		String inputStream = parameterTool.getRequired("input-stream");
		String outputStream = parameterTool.getRequired("output-stream");

		PubsubClient pubsub = new KinesisPubsubClient(parameterTool.getProperties());
		pubsub.createTopic(inputStream, 2, parameterTool.getProperties());
		pubsub.createTopic(outputStream, 2, parameterTool.getProperties());

		// The example job needs to start after streams are created and run in parallel to the validation logic.
		// The thread that runs the job won't terminate, we don't have a job reference to cancel it.
		// Once results are validated, the driver main thread will exit; job/cluster will be terminated from script.
		final AtomicReference<Exception> executeException = new AtomicReference<>();
		Thread executeThread =
			new Thread(
				() -> {
					try {
						KinesisExample.main(args);
						// this message won't appear in the log,
						// job is terminated when shutting down cluster
						LOG.info("executed program");
					} catch (Exception e) {
						executeException.set(e);
					}
				});
		executeThread.start();

		// generate input
		String[] messages = {
			"elephant,5,45218",
			"squirrel,12,46213",
			"bee,3,51348",
			"squirrel,22,52444",
			"bee,10,53412",
			"elephant,9,54867"
		};
		for (String msg : messages) {
			pubsub.sendMessage(inputStream, msg);
		}
		LOG.info("generated records");

		Deadline deadline  = Deadline.fromNow(Duration.ofSeconds(60));
		List<String> results = pubsub.readAllMessages(outputStream);
		while (deadline.hasTimeLeft() && executeException.get() == null && results.size() < messages.length) {
			LOG.info("waiting for results..");
			Thread.sleep(1000);
			results = pubsub.readAllMessages(outputStream);
		}

		if (executeException.get() != null) {
			throw executeException.get();
		}

		LOG.info("results: {}", results);
		Assert.assertEquals("Results received from '" + outputStream + "': " + results,
			messages.length, results.size());

		String[] expectedResults = {
			"elephant,5,45218",
			"elephant,14,54867",
			"squirrel,12,46213",
			"squirrel,34,52444",
			"bee,3,51348",
			"bee,13,53412"
		};

		for (String expectedResult : expectedResults) {
			Assert.assertTrue(expectedResult, results.contains(expectedResult));
		}

		// TODO: main thread needs to create job or CLI fails with:
		// "The program didn't contain a Flink job. Perhaps you forgot to call execute() on the execution environment."
		System.out.println("test finished");
		System.exit(0);
	}

}
