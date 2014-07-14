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

import java.net.InetSocketAddress;

import org.apache.log4j.Level;

import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.client.program.Client;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.util.LogUtils;

public class WordCountLocal {

	private static JobGraph getJobGraph(int sourceSubtasks, int sourceSubtasksPerInstance,
			int counterSubtasks, int counterSubtasksPerInstance, int sinkSubtasks,
			int sinkSubtasksPerInstance) throws Exception {
		JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
		graphBuilder.setSource("WordCountSourceSplitter", WordCountSourceSplitter.class,
				sourceSubtasks, sourceSubtasksPerInstance);
		graphBuilder.setTask("WordCountCounter", WordCountCounter.class, counterSubtasks,
				counterSubtasksPerInstance);
		graphBuilder.setSink("WordCountSink", WordCountSink.class, sinkSubtasks,
				sinkSubtasksPerInstance);

		graphBuilder.fieldsConnect("WordCountSourceSplitter", "WordCountCounter", 0);
		graphBuilder.shuffleConnect("WordCountCounter", "WordCountSink");

		return graphBuilder.getJobGraph();
	}

	private static void wrongArgs() {
		System.out
				.println("USAGE:\n"
						+ "run <local/cluster> <SOURCE num of subtasks> <SOURCE subtasks per instance> <SPLITTER num of subtasks> <SPLITTER subtasks per instance> <COUNTER num of subtasks> <COUNTER subtasks per instance> <SINK num of subtasks> <SINK subtasks per instance>");
	}

	// TODO: arguments check
	public static void main(String[] args) {

		if (args.length != 7) {
			wrongArgs();
		} else {
			LogUtils.initializeDefaultConsoleLogger(Level.ERROR, Level.INFO);

			int sourceSubtasks = 1;
			int sourceSubtasksPerInstance = 1;
			int counterSubtasks = 1;
			int counterSubtasksPerInstance = 1;
			int sinkSubtasks = 1;
			int sinkSubtasksPerInstance = 1;

			try {
				sourceSubtasks = Integer.parseInt(args[1]);
				sourceSubtasksPerInstance = Integer.parseInt(args[2]);
				counterSubtasks = Integer.parseInt(args[3]);
				counterSubtasksPerInstance = Integer.parseInt(args[4]);
				sinkSubtasks = Integer.parseInt(args[5]);
				sinkSubtasksPerInstance = Integer.parseInt(args[6]);
			} catch (Exception e) {
				wrongArgs();
			}

			try {
				JobGraph jG = getJobGraph(sourceSubtasks, sourceSubtasksPerInstance,
						counterSubtasks, counterSubtasksPerInstance, sinkSubtasks,
						sinkSubtasksPerInstance);
				Configuration configuration = jG.getJobConfiguration();

				if (args.length == 0) {
					args = new String[] { "local" };
				}

				if (args[0].equals("local")) {
					System.out.println("Running in Local mode");
					NepheleMiniCluster exec = new NepheleMiniCluster();

					exec.start();

					Client client = new Client(new InetSocketAddress("localhost", 6498),
							configuration);

					client.run(jG, true);

					exec.stop();
				} else if (args[0].equals("cluster")) {
					System.out.println("Running in Cluster mode");

					Client client = new Client(new InetSocketAddress("dell150", 6123),
							configuration);
					client.run(jG, true);
				}

			} catch (Exception e) {
				System.out.println(e);
			}
		}
	}
}
