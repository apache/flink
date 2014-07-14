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

package eu.stratosphere.streaming.test.window.wordcount;

import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;

import eu.stratosphere.test.util.TestBase2;
import eu.stratosphere.types.StringValue;

//TODO: window operator remains unfinished.
public class WindowWordCount extends TestBase2 {

	@Override
	public JobGraph getJobGraph() {
		JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
		graphBuilder.setSource("WindowWordCountSource", WindowWordCountSource.class);
		graphBuilder.setTask("WindowWordCountSplitter", WindowWordCountSplitter.class, 1);
		graphBuilder.setTask("WindowWordCountCounter", WindowWordCountCounter.class, 1);
		graphBuilder.setSink("WindowWordCountSink", WindowWordCountSink.class);

		graphBuilder.broadcastConnect("WindowWordCountSource", "WindowWordCountSplitter");
		graphBuilder.fieldsConnect("WindowWordCountSplitter", "WindowWordCountCounter", 0,
				StringValue.class);
		graphBuilder.broadcastConnect("WindowWordCountCounter", "WindowWordCountSink");

		return graphBuilder.getJobGraph();
	}
}
