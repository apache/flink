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

package eu.stratosphere.test.exampleRecordPrograms;

import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.example.record.triangles.EnumTrianglesOnEdgesWithDegrees;
import eu.stratosphere.test.util.TestBase2;

@RunWith(Parameterized.class)
public class EnumTrianglesOnEdgesWithDegreesITCase extends TestBase2 {
	
	private static final String EDGES_WITH_DEGREES = "1,4|2,3\n1,4|3,5\n1,4|4,2\n1,4|5,3\n2,3|3,5\n2,3|5,3\n3,5|4,2\n3,5|7,2\n5,3|6,1\n3,5|8,2\n7,2|8,2\n";
	private static final String EXPECTED = "2,1,3\n4,1,3\n2,1,5\n7,3,8\n";
	
	protected String edgesPath;
	protected String resultPath; 
	
	public EnumTrianglesOnEdgesWithDegreesITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {
		edgesPath = createTempFile("edgesWithDegrees.txt", EDGES_WITH_DEGREES);
		resultPath = getTempDirPath("triangles");
	}
	
	
	@Override
	protected Plan getTestJob() {
		EnumTrianglesOnEdgesWithDegrees enumTriangles = new EnumTrianglesOnEdgesWithDegrees();
		return enumTriangles.getPlan(
				config.getString("EnumTrianglesTest#NumSubtasks", "4"),
				edgesPath, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED, resultPath);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config = new Configuration();
		config.setInteger("EnumTrianglesTest#NumSubtasks", 4);
		return toParameterList(config);
	}
}
