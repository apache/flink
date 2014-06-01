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
import eu.stratosphere.test.util.RecordAPITestBase;

import eu.stratosphere.examples.scala.graph.TransitiveClosureNaive;

public class TransitiveClosureNaiveITCase extends RecordAPITestBase {

	protected String verticesPath = null;
	protected String edgesPath = null;
	protected String resultPath = null;

	private static final String VERTICES = "0\n1\n2";
	private static final String EDGES = "0|1\n1|2";
	private static final String EXPECTED = "0|0|0\n0|1|1\n0|2|2\n1|1|0\n1|2|1\n2|2|0";

	@Override
	protected void preSubmit() throws Exception {
		verticesPath = createTempFile("vertices.txt", VERTICES);
		edgesPath = createTempFile("edges.txt", EDGES);
		resultPath = getTempDirPath("transitiveClosure");
	}

	@Override
	protected Plan getTestJob() {
		TransitiveClosureNaive transitiveClosureNaive = new TransitiveClosureNaive();
		// "2" is the number of iterations here
		return transitiveClosureNaive.getScalaPlan(DOP, 2, verticesPath, edgesPath, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED, resultPath);
	}
}
