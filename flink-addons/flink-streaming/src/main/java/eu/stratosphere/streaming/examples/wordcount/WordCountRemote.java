/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.examples.wordcount;

import java.io.File;
import java.net.InetSocketAddress;

import org.apache.log4j.Level;

import eu.stratosphere.client.program.Client;
import eu.stratosphere.client.program.JobWithJars;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.util.LogUtils;

public class WordCountRemote {

	private static JobGraph getJobGraph() throws Exception {
		JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
		graphBuilder.setSource("WordCountSource", WordCountDummySource2.class);
		graphBuilder.setTask("WordCountSplitter", WordCountSplitter.class, 2);
		graphBuilder.setTask("WordCountCounter", WordCountCounter.class, 2);
		graphBuilder.setSink("WordCountSink", WordCountSink.class);

		graphBuilder.shuffleConnect("WordCountSource", "WordCountSplitter");
		graphBuilder.fieldsConnect("WordCountSplitter", "WordCountCounter", 0);
		graphBuilder.shuffleConnect("WordCountCounter", "WordCountSink");

		return graphBuilder.getJobGraph();
	}

	public static void main(String[] args) {
		LogUtils.initializeDefaultConsoleLogger(Level.DEBUG, Level.INFO);

		try {
			File file = new File("target/stratosphere-streaming-0.5-SNAPSHOT.jar");
			JobWithJars.checkJarFile(file);

			JobGraph jG = getJobGraph();

			jG.addJar(new Path(file.getAbsolutePath()));

			Configuration configuration = jG.getJobConfiguration();
			Client client = new Client(new InetSocketAddress("hadoop02.ilab.sztaki.hu", 6123), configuration);
			client.run(jG, true);
		} catch (Exception e) {
			System.out.println(e);
		}

	}
}
