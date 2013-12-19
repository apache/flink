/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.test.exampleScalaPrograms;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.examples.scala.graph.ConnectedComponents;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ConnectedComponentsITCase extends eu.stratosphere.test.iterative.ConnectedComponentsITCase {
    public ConnectedComponentsITCase(Configuration config) {
        super(config);
    }

    @Override
    protected Plan getTestJob() {
        int dop = config.getInteger("ConnectedComponents#NumSubtasks", 1);
        int maxIterations = config.getInteger("ConnectedComponents#NumIterations", 1);
        ConnectedComponents cc = new ConnectedComponents();
        Plan plan = cc.getPlan(verticesPath, edgesPath, resultPath, maxIterations);
        plan.setDefaultParallelism(dop);
        return plan;
    }
}
