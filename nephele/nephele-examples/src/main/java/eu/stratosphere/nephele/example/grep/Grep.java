/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.example.grep;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.library.FileLineReader;
import eu.stratosphere.nephele.io.library.FileLineWriter;
import eu.stratosphere.nephele.jobgraph.JobFileInputVertex;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.util.JarFileCreator;

public class Grep {

	public static void main(String[] args) {

		JobGraph jobGraph = new JobGraph("Grep Example Job");

		JobFileInputVertex input = new JobFileInputVertex("Input 1", jobGraph);
		input.setFileInputClass(FileLineReader.class);
		input.setFilePath(new Path("file:///home/ec2-user/test.txt"));
		input.setInstanceType("t1.micro");
		
		JobTaskVertex task1 = new JobTaskVertex("Task 1", jobGraph);
		task1.setTaskClass(GrepTask.class);
		task1.setInstanceType("t1.micro");

		
		JobFileOutputVertex output = new JobFileOutputVertex("Output 1", jobGraph);
		output.setFileOutputClass(FileLineWriter.class);
		output.setFilePath(new Path("file:///tmp/"));
		output.setInstanceType("t1.micro");

		try {

			input.connectTo(task1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			task1.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);

		} catch (JobGraphDefinitionException e) {
			e.printStackTrace();
		}

		// Create jar file and attach it
		final File jarFile = new File("/tmp/grepJob.jar");
		final JarFileCreator jarFileCreator = new JarFileCreator(jarFile);
		jarFileCreator.addClass(GrepTask.class);

		try {
			jarFileCreator.createJarFile();
			System.out.println("done creating!!");
		} catch (IOException ioe) {

			if (jarFile.exists()) {
				jarFile.delete();
			}

			System.out.println("ERROR creating jar");
			return;
		}

		jobGraph.addJar(new Path("file://" + jarFile.getAbsolutePath()));

		// Submit job
		Configuration conf = new Configuration();

		jobGraph.getJobConfiguration().setString("job.cloud.awsaccessid", "xxx");
		jobGraph.getJobConfiguration().setString("job.cloud.awssecretkey", "xxx");
		jobGraph.getJobConfiguration().setString("job.cloud.sshkeypair", "caspeu");
		jobGraph.getJobConfiguration().setString("job.ec2.image.id", "ami-d64474a2");		

		InetSocketAddress jobmanager = new InetSocketAddress("127.0.0.1", 6123);
		
		
		try {
			final JobClient jobClient = new JobClient(jobGraph, conf, jobmanager);
			System.out.println("submitting");
			jobClient.submitJobAndWait();
			System.out.println("done.");
		} catch (Exception e) {
			System.out.println(e);
		}

		if (jarFile.exists()) {
			jarFile.delete();
		}
	}

}
