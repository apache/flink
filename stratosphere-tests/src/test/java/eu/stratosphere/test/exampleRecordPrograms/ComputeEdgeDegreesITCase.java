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

package eu.stratosphere.test.exampleRecordPrograms;

import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.Job;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.example.record.triangles.ComputeEdgeDegrees;
import eu.stratosphere.test.util.TestBase2;

@RunWith(Parameterized.class)
public class ComputeEdgeDegreesITCase extends TestBase2 {
	
	protected String edgesPath = null;
	protected String resultPath = null; 

	private static final String EDGES = "1,2\n1,3\n1,4\n1,5\n2,3\n2,5\n3,4\n3,7\n4,3\n6,5\n8,3\n7,8\n5,6\n";
	private static final String EXPECTED = "1,4|2,3\n1,4|3,5\n1,4|4,2\n1,4|5,3\n2,3|3,5\n2,3|5,3\n3,5|4,2\n3,5|7,2\n5,3|6,1\n3,5|8,2\n7,2|8,2\n";
	
	public ComputeEdgeDegreesITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {
		edgesPath = createTempFile("edges.txt", EDGES);
		resultPath = getTempDirPath("edgesWithDegrees");
	}

	@Override
	protected Job getTestJob() {
		ComputeEdgeDegrees computeDegrees = new ComputeEdgeDegrees();
		return computeDegrees.createJob(config.getString("ComputeEdgeDegreesTest#NumSubtasks", "4"),
				edgesPath, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED, resultPath);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config = new Configuration();
		config.setInteger("ComputeEdgeDegreesTest#NumSubtasks", 4);
		return toParameterList(config);
	}
}
