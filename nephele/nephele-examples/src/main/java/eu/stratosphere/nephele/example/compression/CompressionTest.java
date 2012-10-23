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

package eu.stratosphere.nephele.example.compression;

import java.io.IOException;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.library.DirectoryReader;
import eu.stratosphere.nephele.io.library.DirectoryWriter;
import eu.stratosphere.nephele.jobgraph.JobFileInputVertex;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;

public class CompressionTest {

	public static void main(String[] args) {

		// Create job graph
		JobGraph jobGraph = new JobGraph("Compression Test Job");

		JobFileInputVertex input = new JobFileInputVertex("Input 1", jobGraph);
		input.setFileInputClass(DirectoryReader.class);
		input.setFilePath(new Path("file:///home/akli/trainingfiles"));

		JobTaskVertex task1 = new JobTaskVertex("Task 1", jobGraph);
		task1.setTaskClass(CompressionTestTask.class);

		JobFileOutputVertex output = new JobFileOutputVertex("Output 1", jobGraph);
		output.setFileOutputClass(DirectoryWriter.class);
		output.setFilePath(new Path("file:///home/akli/compOutput"));

		try {

			input.connectTo(task1, ChannelType.FILE, CompressionLevel.MEDIUM_COMPRESSION);
			// task1.connectTo(task2, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			task1.connectTo(output, ChannelType.FILE, CompressionLevel.MEDIUM_COMPRESSION);
		} catch (JobGraphDefinitionException e) {
			e.printStackTrace();
			return;
		}

		// Create configuration for the job
		// Configuration jobConfiguration = new Configuration();
		// jobConfiguration.setString("job.cloud.username", "admin");
		// jobConfiguration.setString("job.cloud.privatekey",
		// loadFile("/home/warneke/.euca/euca2-admin-b03fc65d-pk.pem2"));
		// jobConfiguration.setString("job.cloud.certificate",
		// loadFile("/home/warneke/.euca/euca2-admin-b03fc65d-cert.pem"));

		// jobGraph.setJobConfiguration(jobConfiguration);
		jobGraph.addJar(new Path("file:///home/akli/Compression.jar"));

		Configuration clientConfiguration = new Configuration();
		clientConfiguration.setString("jobmanager.rpc.port", "6023");
		clientConfiguration.setString("jobmanager.rpc.address", "192.168.2.111");

		try {
			JobClient jobClient = new JobClient(jobGraph, clientConfiguration);

			jobClient.submitJobAndWait();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (JobExecutionException e) {
			e.printStackTrace();
		}
	}
}
