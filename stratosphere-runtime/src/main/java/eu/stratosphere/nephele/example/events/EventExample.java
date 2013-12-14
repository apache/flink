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

/*
 *  Copyright 2010 casp.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */
package eu.stratosphere.nephele.example.events;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.library.FileLineReader;
import eu.stratosphere.nephele.io.library.FileLineWriter;
import eu.stratosphere.nephele.jobgraph.JobFileInputVertex;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;

import java.io.IOException;

/**
 * @author casp
 */
public class EventExample {

	public static void main(String[] args) {

		JobGraph jobGraph = new JobGraph("Grep Example Job");

		JobFileInputVertex input = new JobFileInputVertex("Input 1", jobGraph);
		input.setFileInputClass(FileLineReader.class);
		input.setFilePath(new Path("file:///Users/casp/test2.txt"));

		JobTaskVertex task1 = new JobTaskVertex("Task 1", jobGraph);
		task1.setTaskClass(EventSender.class);

		JobTaskVertex task2 = new JobTaskVertex("Task 2", jobGraph);
		task2.setTaskClass(EventReceiver.class);

		JobFileOutputVertex output = new JobFileOutputVertex("Output 1", jobGraph);
		output.setFileOutputClass(FileLineWriter.class);
		output.setFilePath(new Path("file:///Users/casp/output.txt"));

		jobGraph.addJar(new Path("file:///Users/casp/EventTask.jar"));
		jobGraph.addJar(new Path("file:///Users/casp/StringTaskEvent.jar"));
		try {

			input.connectTo(task1, ChannelType.INMEMORY);
			task1.connectTo(task2, ChannelType.INMEMORY);
			task2.connectTo(output, ChannelType.INMEMORY);

		} catch (JobGraphDefinitionException e) {
			e.printStackTrace();
		}

		Configuration conf = new Configuration();
		conf.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "127.0.0.1");
		conf.setString(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, "6023");

		try {
			JobClient jobClient = new JobClient(jobGraph, conf);
			JobSubmissionResult result = jobClient.submitJob();
			System.out.println(result.getDescription());
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

	}
}
