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

package eu.stratosphere.pact.test.scalaPactPrograms;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.scala.examples.datamining.KMeans;

@RunWith(Parameterized.class)
public class IterativeKMeansITCase extends eu.stratosphere.pact.test.iterative.IterativeKMeansITCase {

	public IterativeKMeansITCase(Configuration config) {
		super(config);
	}

	@Override
	protected Plan getPactPlan() {

		KMeans kmi = new KMeans();

		Plan plan = kmi.getPlan(
				Integer.parseInt(config.getString("IterativeKMeansITCase#NumIterations", "1")),
				dataPath,
				clusterPath,
				resultPath);
		plan.setDefaultParallelism(config.getInteger("IterativeKMeansITCase#NoSubtasks", 1));
		return plan;
	}
}
