/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.example.speedtest;

import java.io.File;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobGenericInputVertex;
import eu.stratosphere.nephele.jobgraph.JobGenericOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.util.JarFileCreator;

/**
 * This class implements a speed test for Nephele. It's primary purpose is to benchmark the performance of Nephele's
 * network channels with different degrees of parallelism.
 * 
 * @author warneke
 */
public final class SpeedTest {

	/**
	 * Configuration key to specify the amount of data to be send in GB.
	 */
	static final String DATA_VOLUME_CONFIG_KEY = "data.volume";

	/**
	 * Entry point to the application.
	 * 
	 * @param args
	 *        the provided arguments
	 */
	public static void main(final String[] args) {

		// Parse the arguments first
		if (args.length < 4) {
			System.err
				.println("Insufficient number of arguments. Please provide <job manager address> <amount of data to send in GB> <number of subtasks> <number of subtasks per task> (<use forwarder>)");
			System.exit(1);
			return;
		}

		// Determine the job manager address
		final String jobManagerAddress = args[0];

		// Determine amount of data to send in GB
		int amountOfDataToSend = 0;
		try {
			amountOfDataToSend = Integer.parseInt(args[1]);
		} catch (NumberFormatException e) {
			System.err.println("Cannot parse amount of data to send. Please provide a positive integer value.");
			System.exit(1);
			return;
		}

		if (amountOfDataToSend <= 0 || amountOfDataToSend > 1024) {
			System.err
				.println("Please provide an integer value between 1 and 1024 indicating the amount of data to send in GB.");
			System.exit(1);
			return;
		}

		// Determine the number of subtasks
		int numberOfSubtasks = 0;
		try {
			numberOfSubtasks = Integer.parseInt(args[2]);
		} catch (NumberFormatException e) {
			System.err.println("Cannot parse the number of subtasks. Please provide a positive integer value.");
			System.exit(1);
			return;
		}

		if (numberOfSubtasks <= 0) {
			System.err.println("Please provide a positive integer value indicating the number of subtasks.");
			System.exit(1);
			return;
		}

		// Determine the number of subtasks per instance
		int numberOfSubtasksPerInstance = 0;
		try {
			numberOfSubtasksPerInstance = Integer.parseInt(args[3]);
		} catch (NumberFormatException e) {
			System.err
				.println("Cannot parse the number of subtasks per instance. Please provide a positive integer value.");
			System.exit(1);
			return;
		}

		if (numberOfSubtasksPerInstance <= 0) {
			System.err
				.println("Please provide a positive integer value indicating the number of subtasks per instance.");
			System.exit(1);
			return;
		}

		// Determine whether to use a forwarder or not
		boolean useForwarder = false;
		if (args.length >= 5) {
			useForwarder = Boolean.parseBoolean(args[4]);
		}

		final JobGraph jobGraph = new JobGraph("Nephele Speed Test");

		final JobGenericInputVertex producer = new JobGenericInputVertex("Speed Test Producer", jobGraph);
		producer.setInputClass(SpeedTestProducer.class);
		producer.setNumberOfSubtasks(numberOfSubtasks);
		producer.setNumberOfSubtasksPerInstance(numberOfSubtasksPerInstance);
		producer.getConfiguration().setInteger(DATA_VOLUME_CONFIG_KEY, amountOfDataToSend);

		JobTaskVertex forwarder = null;
		if (useForwarder) {
			forwarder = new JobTaskVertex("Speed Test Forwarder", jobGraph);
			forwarder.setTaskClass(SpeedTestForwarder.class);
			forwarder.setNumberOfSubtasks(numberOfSubtasks);
			forwarder.setNumberOfSubtasksPerInstance(numberOfSubtasksPerInstance);
		}

		final JobGenericOutputVertex consumer = new JobGenericOutputVertex("Speed Test Consumer", jobGraph);
		consumer.setOutputClass(SpeedTestConsumer.class);
		consumer.setNumberOfSubtasks(numberOfSubtasks);
		consumer.setNumberOfSubtasksPerInstance(numberOfSubtasksPerInstance);

		// Set vertex sharing
		producer.setVertexToShareInstancesWith(consumer);
		if (forwarder != null) {
			forwarder.setVertexToShareInstancesWith(consumer);
		}

		// Connect the vertices
		try {
			if (forwarder == null) {
				producer.connectTo(consumer, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION,
					DistributionPattern.BIPARTITE);
			} else {
				producer.connectTo(forwarder, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION,
					DistributionPattern.BIPARTITE);
				forwarder.connectTo(consumer, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION,
					DistributionPattern.BIPARTITE);
			}

		} catch (JobGraphDefinitionException e) {
			e.printStackTrace();
			System.exit(1);
			return;
		}

		File jarFile = null;

		try {

			// Create jar file of job
			jarFile = File.createTempFile("speedtest", "jar");
			jarFile.deleteOnExit();

			final JarFileCreator jfc = new JarFileCreator(jarFile);
			jfc.addClass(SpeedTest.class);
			jfc.addClass(SpeedTestProducer.class);
			jfc.addClass(SpeedTestForwarder.class);
			jfc.addClass(SpeedTestConsumer.class);
			jfc.addClass(SpeedTestRecord.class);
			jfc.createJarFile();

			jobGraph.addJar(new Path("file://" + jarFile.getAbsolutePath()));

			final Configuration clientConfiguration = new Configuration();
			clientConfiguration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jobManagerAddress);
			clientConfiguration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

			final JobClient jobClient = new JobClient(jobGraph, clientConfiguration);
			final long executionTime = jobClient.submitJobAndWait();

			// Calculate throughput in MBit/s and output it
			System.out.print("Job finished with a throughput of " + toMBitPerSecond(amountOfDataToSend, executionTime));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Calculates the throughput in MBit/s from the given amount of data that has been sent and the execution time.
	 * 
	 * @param amountOfDataToSend
	 *        the amount of data that has been sent in GB
	 * @param executionTime
	 *        the execution time in milliseconds
	 * @return the resulting throughput in MBit/s
	 */
	private static int toMBitPerSecond(final int amountOfDataToSend, final long executionTime) {

		final double dataVolumeInMBit = amountOfDataToSend * 8192.0;
		final double executionTimeInSeconds = executionTime / 1000.0;

		return (int) Math.round(dataVolumeInMBit / executionTimeInSeconds);
	}
}
