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

package eu.stratosphere.nephele.example.union;

import java.io.File;
import java.io.IOException;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobFileInputVertex;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.util.JarFileCreator;

public class UnionJob {

	public static void main(final String[] args) {

		// Create graph and define vertices
		final JobGraph unionGraph = new JobGraph("Union Job");

		final JobFileInputVertex input1 = new JobFileInputVertex("Input 1", unionGraph);
		input1.setFileInputClass(ProducerTask.class);
		input1.setFilePath(new Path("file:///tmp/"));

		final JobFileInputVertex input2 = new JobFileInputVertex("Input 2", unionGraph);
		input2.setFileInputClass(ProducerTask.class);
		input2.setFilePath(new Path("file:///tmp/"));

		final JobTaskVertex union = new JobTaskVertex("Union", unionGraph);
		union.setTaskClass(UnionTask.class);

		final JobFileOutputVertex output = new JobFileOutputVertex("Output", unionGraph);
		output.setFileOutputClass(ConsumerTask.class);
		output.setFilePath(new Path("file:///tmp/"));

		// Create edges between vertices
		try {
			input1.connectTo(union, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			input2.connectTo(union, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
			union.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
		} catch (JobGraphDefinitionException e) {
			e.printStackTrace();
			return;
		}

		// Create jar file and attach it
		final File jarFile = new File("/tmp/unionJob.jar");
		final JarFileCreator jarFileCreator = new JarFileCreator(jarFile);
		jarFileCreator.addClass(ProducerTask.class);
		jarFileCreator.addClass(UnionTask.class);
		jarFileCreator.addClass(ConsumerTask.class);
		
		try {
			jarFileCreator.createJarFile();
		} catch (IOException ioe) {

			ioe.printStackTrace();

			if (jarFile.exists()) {
				jarFile.delete();
			}

			return;
		}
		
		//Define instance sharing
		input1.setVertexToShareInstancesWith(output);
		input2.setVertexToShareInstancesWith(output);
		union.setVertexToShareInstancesWith(output);

		unionGraph.addJar(new Path("file://" + jarFile.getAbsolutePath()));

		final Configuration conf = new Configuration();
		conf.setString("jobmanager.rpc.address", "localhost");
		
		try {
			final JobClient jobClient = new JobClient(unionGraph, conf);
			jobClient.submitJobAndWait();
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (jarFile.exists()) {
			jarFile.delete();
		}
	}
}
