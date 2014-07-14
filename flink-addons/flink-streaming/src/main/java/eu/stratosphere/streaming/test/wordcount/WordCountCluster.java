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

package eu.stratosphere.streaming.test.wordcount;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;

import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.client.program.JobWithJars;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.LogUtils;

public class WordCountCluster {


	protected final Configuration config;

	
	public WordCountCluster() {
		this(new Configuration());
	}

	public WordCountCluster(Configuration config) {
		this.config = config;

		LogUtils.initializeDefaultConsoleLogger(Level.WARN);
	}

	private static JobGraph getJobGraph() throws Exception {
		JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
		graphBuilder.setSource("WordCountSource", WordCountDummySource.class);
		graphBuilder.setTask("WordCountSplitter", WordCountSplitter.class, 2);
		graphBuilder.setTask("WordCountCounter", WordCountCounter.class, 2);
		graphBuilder.setSink("WordCountSink", WordCountSink.class);

		graphBuilder.shuffleConnect("WordCountSource", "WordCountSplitter");
		graphBuilder.fieldsConnect("WordCountSplitter", "WordCountCounter", 0,
				StringValue.class);
		graphBuilder.shuffleConnect("WordCountCounter", "WordCountSink");

		return graphBuilder.getJobGraph();
	}

	public static void main(String[] args) {
		
		try {

			JobGraph jG = getJobGraph();
			
			int jobManagerRpcPort = 6123;
			Configuration configuration = jG.getJobConfiguration();
			configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "hadoop02.ilab.sztaki.hu");
			configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerRpcPort);
			
			
			JobClient client= new JobClient(jG, configuration);
			
			
			//JobClient client = exec.getJobClient(jG);

			ClassLoader userClassLoader;
			
			
			
			client.submitJobAndWait();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
