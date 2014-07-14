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

import org.apache.log4j.Level;

import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;
import eu.stratosphere.streaming.util.ClusterUtil;
import eu.stratosphere.streaming.util.LogUtils;

public class WordCountLocal {

	public static JobGraph getJobGraph() {

		JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph", FaultToleranceType.NONE);
		graphBuilder.setSource("WordCountSourceSplitter", new WordCountSourceSplitter("src/test/resources/testdata/hamlet.txt"));
		graphBuilder.setTask("WordCountCounter", new WordCountCounter());
		graphBuilder.setSink("WordCountSink", new WordCountSink());

		graphBuilder.fieldsConnect("WordCountSourceSplitter", "WordCountCounter", 0);
		graphBuilder.shuffleConnect("WordCountCounter", "WordCountSink");

		return graphBuilder.getJobGraph();
	}

	public static void main(String[] args) {

		LogUtils.initializeDefaultConsoleLogger(Level.DEBUG, Level.INFO);

		if (args.length == 0) {
			args = new String[] { "local" };
		}

		if (args[0].equals("local")) {
			ClusterUtil.runOnMiniCluster(getJobGraph());

		} else if (args[0].equals("cluster")) {
			ClusterUtil.runOnLocalCluster(getJobGraph(), "hadoop02.ilab.sztaki.hu", 6123);
		}

	}
}
