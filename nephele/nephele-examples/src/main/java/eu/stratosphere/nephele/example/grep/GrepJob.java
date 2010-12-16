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

public class GrepJob implements Job {

	JobGraph jobGraph = null;

	public GrepJob() {

		jobGraph = new JobGraph("Grep Example Job");

		JobFileInputVertex input = new JobFileInputVertex("Input 1", jobGraph);
		input.setFileInputClass(FileLineReader.class);
		input.setFilePath(new Path("hdfs://localhost:8000/user/wenjun/input1"));

		JobTaskVertex task1 = new JobTaskVertex("Task 1", jobGraph);
		task1.setTaskClass(GrepTask.class);

		JobTaskVertex task2 = new JobTaskVertex("Task 2", jobGraph);
		task2.setTaskClass(GrepTask.class);

		JobFileOutputVertex output = new JobFileOutputVertex("Output 1", jobGraph);
		output.setFileOutputClass(FileLineWriter.class);
		output.setFilePath(new Path("hdfs://localhost:8000/user/wenjun/output1"));

		task1.setNumberOfSubtasks(2);

		try {

			input.connectTo(task1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			task1.connectTo(task2, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			task2.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);

		} catch (JobGraphDefinitionException e) {
			e.printStackTrace();
		}

	}

	public JobGraph getJobGraph() {
		return jobGraph;
	}
}
