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

package eu.stratosphere.streaming.examples.window.sum;

import org.apache.log4j.Level;

import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.util.ClusterUtil;
import eu.stratosphere.streaming.util.LogUtils;

public class WindowSumLocal {

	public static JobGraph getJobGraph() {
		JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
		graphBuilder.setSource("WindowSumSource", WindowSumSource.class);
		graphBuilder.setTask("WindowSumMultiple", WindowSumMultiple.class, 1, 1);
		graphBuilder.setTask("WindowSumAggregate", WindowSumAggregate.class, 1, 1);
		graphBuilder.setSink("WindowSumSink", WindowSumSink.class);

		graphBuilder.shuffleConnect("WindowSumSource", "WindowSumMultiple");
		graphBuilder.shuffleConnect("WindowSumMultiple", "WindowSumAggregate");
		graphBuilder.shuffleConnect("WindowSumAggregate", "WindowSumSink");

		return graphBuilder.getJobGraph();
	}

	public static void main(String[] args) {

		LogUtils.initializeDefaultConsoleLogger(Level.DEBUG, Level.INFO);
		ClusterUtil.runOnMiniCluster(getJobGraph());

	}
}
