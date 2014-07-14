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

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.junit.Assert;

import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.LogUtils;

public class WordCountLocal {
	
	private static final int MINIMUM_HEAP_SIZE_MB = 192;

	protected final Configuration config;
	
	private NepheleMiniCluster executor;
		
	public WordCountLocal() {
		this(new Configuration());
	}
	
	public WordCountLocal(Configuration config) {
		verifyJvmOptions();
		this.config = config;
		
		LogUtils.initializeDefaultConsoleLogger(Level.WARN);
	}
	

	private void verifyJvmOptions() {
		long heap = Runtime.getRuntime().maxMemory() >> 20;
		Assert.assertTrue("Insufficient java heap space " + heap + "mb - set JVM option: -Xmx" + MINIMUM_HEAP_SIZE_MB
				+ "m", heap > MINIMUM_HEAP_SIZE_MB - 50);
	}
	

	public void startCluster() throws Exception {
		this.executor = new NepheleMiniCluster();
		this.executor.setDefaultOverwriteFiles(true);
		this.executor.start();
	}

	public void stopCluster() throws Exception {
		try {
			if (this.executor != null) {
				this.executor.stop();
				this.executor = null;
				FileSystem.closeAll();
				System.gc();
			}
		} finally {
		}
	}

	public void runJob() throws Exception {
		// submit job
		JobGraph jobGraph = null;
		try {
			jobGraph = getJobGraph();
		}
		catch(Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Failed to obtain JobGraph!");
		}
		
		Assert.assertNotNull("Obtained null JobGraph", jobGraph);
		
		try {
			JobClient client = null;
			try {
			client = this.executor.getJobClient(jobGraph); }
			catch(Exception e) {
				System.err.println("here");
			}
			client.submitJobAndWait();
		}
		catch(Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Job execution failed!");
		}
	}
	

	protected JobGraph getJobGraph() throws Exception {
		JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
		graphBuilder.setSource("WordCountSource", WordCountSource.class);
		graphBuilder.setTask("WordCountSplitter", WordCountSplitter.class, 2);
		graphBuilder.setTask("WordCountCounter", WordCountCounter.class, 2);
		graphBuilder.setSink("WordCountSink", WordCountSink.class);

		graphBuilder.shuffleConnect("WordCountSource", "WordCountSplitter");
		graphBuilder.fieldsConnect("WordCountSplitter", "WordCountCounter", 0,
				StringValue.class);
		graphBuilder.shuffleConnect("WordCountCounter", "WordCountSink");

		return graphBuilder.getJobGraph();
	}
	
	
	
	public static void main(String[] args){
		WordCountLocal wC = new WordCountLocal();
		
		try {
			wC.startCluster();
			wC.runJob();
			wC.stopCluster();
		} catch (Exception e) {}
	}
}
