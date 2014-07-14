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

package eu.stratosphere.streaming.test.cellinfo;

import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.test.util.TestBase2;
import eu.stratosphere.types.IntValue;

public class CellInfo extends TestBase2 {

  @Override
  public JobGraph getJobGraph() {
    JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
    graphBuilder.setSource("infoSource", InfoSourceInvokable.class);
    graphBuilder.setSource("querySource", QuerySourceInvokable.class);
    graphBuilder.setTask("cellTask", CellTaskInvokable.class, 2);
    graphBuilder.setSink("sink", CellSinkInvokable.class);
    
    graphBuilder.fieldsConnect("infoSource", "cellTask", 0, IntValue.class);
    graphBuilder.fieldsConnect("querySource", "cellTask",0, IntValue.class);
    graphBuilder.broadcastConnect("cellTask", "sink");

    return graphBuilder.getJobGraph();
  }

}
