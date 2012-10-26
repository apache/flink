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

package eu.stratosphere.nephele.example.broadcast;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.util.JarFileCreator;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class creates and submits a simple broadcast test job.
 * 
 * @author warneke
 */
public class BroadcastJob {

	/**
	 * The address of the job manager.
	 */
	private static String JOB_MANAGER_ADDRESS = null;

	/**
	 * The type of instances the job is supposed to run on.
	 */
	private static String INSTANCE_TYPE = null;

	/**
	 * The number of times the job execution shall be repeated.
	 */
	private static int NUMBER_OF_RUNS = -1;

	/**
	 * The number of consumers to create for the job execution.
	 */
	private static int NUMBER_OF_CONSUMERS = -1;

	/**
	 * The topology tree to use during the job execution.
	 */
	private static String TOPOLOGY_TREE = null;

	/**
	 * The number of records to send in one run.
	 */
	private static int NUMBER_OF_RECORDS = -1;

	/**
	 * The path to write the output to.
	 */
	private static String OUTPUT_PATH = null;

	/**
	 * The JAR file with the classes required to run the job
	 */
	private static File JAR_FILE = null;

	/**
	 * The entry point to the application.
	 * 
	 * @param args
	 *        the command line arguments
	 */
	public static void main(String[] args) {

		// Parse the input parameters
		if (args.length < 7) {
			System.err.println("Please specify at least the following parameters:");
			System.err
				.println("<address of job manager> <instance type> <topology tree> <number of runs> <number of consumers> <number of records> <output path>");
			System.exit(-1);
		}

		// Parse the job manager address
		JOB_MANAGER_ADDRESS = args[0];

		// Parse the instance type
		INSTANCE_TYPE = args[1];

		// Parse topology tree
		TOPOLOGY_TREE = args[2];

		// Parse the number of runs
		try {
			NUMBER_OF_RUNS = Integer.parseInt(args[3]);
		} catch (NumberFormatException nfe) {
			System.err.println("Invalid number of runs specified: " + args[3]);
			System.exit(-1);
		}
		if (NUMBER_OF_RUNS <= 0) {
			System.err.println("Number of runs must be greater than 0");
			System.exit(-1);
		}

		// Parse number of consumer
		try {
			NUMBER_OF_CONSUMERS = Integer.parseInt(args[4]);
		} catch (NumberFormatException nfe) {
			System.err.println("Invalid number of consumers specified: " + args[4]);
			System.exit(-1);
		}
		if (NUMBER_OF_CONSUMERS <= 0) {
			System.err.println("Number of consumers must be greater than 0");
			System.exit(-1);
		}

		// Parse number of records to send
		try {
			NUMBER_OF_RECORDS = Integer.parseInt(args[5]);
		} catch (NumberFormatException nfe) {
			System.err.println("Invalid number of records specified: " + args[5]);
			System.exit(-1);
		}

		if (NUMBER_OF_RECORDS <= 0) {
			System.err.println("Number of records to send must be greater or equal to 0");
			System.exit(-1);
		}

		// Parse output path
		OUTPUT_PATH = args[6];

		// Prepare jar file
		JAR_FILE = new File("/tmp/broadcastJob.jar");
		final JarFileCreator jarFileCreator = new JarFileCreator(JAR_FILE);
		jarFileCreator.addClass(BroadcastProducer.class);
		jarFileCreator.addClass(BroadcastConsumer.class);
		jarFileCreator.addClass(BroadcastRecord.class);

		try {
			jarFileCreator.createJarFile();
		} catch (IOException ioe) {

			if (JAR_FILE.exists()) {
				JAR_FILE.delete();
			}

			System.err.println("Error creating jar file: " + StringUtils.stringifyException(ioe));
			System.exit(-1);
		}

		try {
			final BufferedWriter throughputWriter = new BufferedWriter(new FileWriter(getThroughputFilename()));
			final BufferedWriter durationWriter = new BufferedWriter(new FileWriter(getDurationFilename()));

			// Execute the individual job runs
			for (int i = 0; i < NUMBER_OF_RUNS; ++i) {
				try {
					runJob(i, throughputWriter, durationWriter);
				} catch (Exception e) {
					System.err.println("Error executing run " + i + ": " + StringUtils.stringifyException(e));
					break;
				}
			}

			throughputWriter.close();
			durationWriter.close();
		} catch (IOException ioe) {
			System.err.println("An IO exception occurred " + StringUtils.stringifyException(ioe));
		}

		// Delete jar file
		if (JAR_FILE.exists()) {
			JAR_FILE.delete();
		}
	}

	/**
	 * Executes a specific run of the job.
	 * 
	 * @param run
	 *        the run of the job
	 * @param throughputWriter
	 *        writer object to write the throughput results for each run
	 * @param durationWriter
	 *        writer object to write the duration results for each run
	 */
	private static void runJob(final int run, final BufferedWriter throughputWriter, final BufferedWriter durationWriter)
			throws JobGraphDefinitionException, IOException, InterruptedException, JobExecutionException {

		// Construct job graph
		final JobGraph jobGraph = new JobGraph("Broadcast Job (Run " + run + ")");

		final JobInputVertex producer = new JobInputVertex("Broadcast Producer", jobGraph);
		producer.setInputClass(BroadcastProducer.class);
		producer.setInstanceType(INSTANCE_TYPE);
		producer.getConfiguration().setInteger(BroadcastProducer.NUMBER_OF_RECORDS_KEY, NUMBER_OF_RECORDS);
		producer.getConfiguration().setInteger(BroadcastProducer.RUN_KEY, run);
		producer.setNumberOfSubtasks(1);
		producer.setNumberOfSubtasksPerInstance(1);

		final JobOutputVertex consumer = new JobOutputVertex("Broadcast Consumer", jobGraph);
		consumer.setOutputClass(BroadcastConsumer.class);
		consumer.setNumberOfSubtasks(NUMBER_OF_CONSUMERS);
		consumer.setNumberOfSubtasksPerInstance(1);
		consumer.setInstanceType(INSTANCE_TYPE);
		consumer.getConfiguration().setInteger(BroadcastProducer.RUN_KEY, run);
		consumer.getConfiguration().setString(BroadcastConsumer.OUTPUT_PATH_KEY, OUTPUT_PATH);
		consumer.getConfiguration().setString(BroadcastConsumer.TOPOLOGY_TREE_KEY, TOPOLOGY_TREE);
		consumer.getConfiguration().setString(BroadcastConsumer.INSTANCE_TYPE_KEY, INSTANCE_TYPE);
		consumer.getConfiguration().setInteger(BroadcastProducer.NUMBER_OF_RECORDS_KEY, NUMBER_OF_RECORDS);

		// Connect both vertices
		producer.connectTo(consumer, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);

		// Attach jar file
		jobGraph.addJar(new Path("file://" + JAR_FILE.getAbsolutePath()));

		// Submit job
		Configuration conf = new Configuration();
		conf.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, JOB_MANAGER_ADDRESS);
		conf.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

		final JobClient jobClient = new JobClient(jobGraph, conf);
		final long jobDuration = jobClient.submitJobAndWait();

		final long numberOfBytesSent = (long) BroadcastRecord.RECORD_SIZE * (long) NUMBER_OF_RECORDS
			* (long) NUMBER_OF_CONSUMERS;
		// Throughput in bits per second
		final double throughput = (double) (numberOfBytesSent * 1000L * 8L) / (double) (jobDuration * 1024L * 1024L);

		// Write calculated throughput to file
		throughputWriter.write(throughput + "\n");

		// Write the job duration
		durationWriter.write(jobDuration + "\n");
	}

	/**
	 * Constructs the filename for the throughput result.
	 * 
	 * @return the filename for the throughput result
	 */
	private static String getThroughputFilename() {

		return OUTPUT_PATH + File.separator + "throughput_" + INSTANCE_TYPE + "_" + TOPOLOGY_TREE + "_"
			+ NUMBER_OF_CONSUMERS + "_" + NUMBER_OF_RECORDS + ".dat";
	}

	private static String getDurationFilename() {

		return OUTPUT_PATH + File.separator + "duration_" + INSTANCE_TYPE + "_" + TOPOLOGY_TREE + "_"
			+ NUMBER_OF_CONSUMERS + "_" + NUMBER_OF_RECORDS + ".dat";
	}
}
