/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.runtime.iterative.playing.events;

import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
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
import eu.stratosphere.pact.runtime.iterative.playing.JobGraphUtils;
import eu.stratosphere.pact.runtime.iterative.playing.PlayConstants;

import java.io.IOException;

public class Events {

  public static void main(String[] args) throws JobExecutionException, IOException, JobGraphDefinitionException {

    JobGraph jobGraph = new JobGraph("Events");

    JobFileInputVertex input = new JobFileInputVertex("Input 1", jobGraph);
    input.setFileInputClass(FileLineReader.class);
    input.setFilePath(new Path("file://" + PlayConstants.PLAY_DIR + "test-inputs/simple/1.txt"));

    JobTaskVertex task1 = new JobTaskVertex("Sender", jobGraph);
    task1.setTaskClass(Sender.class);

    JobTaskVertex task2 = new JobTaskVertex("Receiver", jobGraph);
    task2.setTaskClass(Receiver.class);

    JobFileOutputVertex output = new JobFileOutputVertex("Output 1", jobGraph);
    output.setFileOutputClass(FileLineWriter.class);
    output.setFilePath(new Path("file:///tmp/stratosphere/output.txt"));

    input.connectTo(task1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
    task1.connectTo(task2, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
    task2.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);

    input.setVertexToShareInstancesWith(task1);
    task2.setVertexToShareInstancesWith(task1);
    output.setVertexToShareInstancesWith(task1);


    GlobalConfiguration.loadConfiguration(PlayConstants.PLAY_DIR + "local-conf");
    Configuration conf = GlobalConfiguration.getConfiguration();

    JobGraphUtils.submit(jobGraph, conf);
  }


}
