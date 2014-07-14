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
package eu.stratosphere.streaming.examples.basictopology;

import org.apache.log4j.Level;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.ArrayStreamRecord;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.util.ClusterUtil;
import eu.stratosphere.streaming.util.LogUtils;

public class BasicTopology {

	public static class BasicSource extends UserSourceInvokable {

		private static final long serialVersionUID = 1L;
		StreamRecord record = (new ArrayStreamRecord(1)).setTuple(0, new Tuple1<String>("streaming"));

		@Override
		public void invoke() throws Exception {

			while (true) {
				// continuously emit records
				emit(record);
				performanceCounter.count();
			}

		}
	}

	public static class BasicTask extends UserTaskInvokable {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(StreamRecord record) throws Exception {
			// send record to sink without any modifications
			emit(record);
			performanceCounter.count();
		}

	}

	public static class BasicSink extends UserSinkInvokable {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(StreamRecord record) throws Exception {
			// do nothing
			System.out.println(record.getTuple(0).getField(0));
		}
	}

	private static JobGraph getJobGraph() {
		JobGraphBuilder graphBuilder = new JobGraphBuilder("BasicStreamingTopology");
		graphBuilder.setSource("BasicSource", new BasicSource(), 1, 1);
		graphBuilder.setTask("BasicTask", new BasicTask(), 1, 1);
		graphBuilder.setSink("BasicSink", new BasicSink(), 1, 1);

		graphBuilder.shuffleConnect("BasicSource", "BasicTask");
		graphBuilder.shuffleConnect("BasicTask", "BasicSink");

		return graphBuilder.getJobGraph();
	}

	public static void main(String[] args) {

		// set logging parameters for local run
		LogUtils.initializeDefaultConsoleLogger(Level.INFO, Level.INFO);

		ClusterUtil.runOnMiniCluster(getJobGraph());
	}
}
