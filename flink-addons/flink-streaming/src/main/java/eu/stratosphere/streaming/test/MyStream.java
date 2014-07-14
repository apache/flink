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

package eu.stratosphere.streaming.test;

import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.api.JobGraphBuilder.Partitioning;
import eu.stratosphere.test.util.TestBase2;

public class MyStream extends TestBase2 {

  @Override
  public JobGraph getJobGraph() {
    JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
    graphBuilder.setSource("infoSource", TestSourceInvokable.class);
    graphBuilder.setSource("querySource", QuerySourceInvokable.class);
    graphBuilder.setTask("cellTask", TestTaskInvokable.class, 2);
    graphBuilder.setSink("sink", TestSinkInvokable.class);

    graphBuilder.connect("infoSource", "cellTask", Partitioning.BROADCAST,
        ChannelType.INMEMORY);
    graphBuilder.connect("querySource", "cellTask", Partitioning.BROADCAST,
        ChannelType.INMEMORY);
    graphBuilder.connect("cellTask", "sink", Partitioning.BROADCAST,
        ChannelType.INMEMORY);

    return graphBuilder.getJobGraph();
  }

}
