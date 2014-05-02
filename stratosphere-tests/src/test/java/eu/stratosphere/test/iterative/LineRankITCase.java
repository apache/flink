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

package eu.stratosphere.test.iterative;

import java.util.Collection;

import eu.stratosphere.test.util.RecordAPITestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.examples.scala.graph.LineRank;

@RunWith(Parameterized.class)
public class LineRankITCase extends RecordAPITestBase {
	
	private static final String SOURCE_INCIDENCE = "1,1,1\n" +
	                                               "2,1,1\n" +
	                                               "3,1,1\n" +
	                                               "4,2,1\n" +
	                                               "5,3,1\n" +
	                                               "6,3,1\n" +
	                                               "7,4,1\n" +
	                                               "8,4,1\n" +
	                                               "9,5,1\n";
	
	private static final String TARGET_INCIDENCE = "1,2,1\n" +
	                                               "2,3,1\n" +
	                                               "3,4,1\n" +
	                                               "4,3,1\n" +
	                                               "5,2,1\n" +
	                                               "6,5,1\n" +
	                                               "7,1,1\n" +
	                                               "8,3,1\n" +
	                                               "9,4,1\n";

	protected String sourcesPath;
	protected String targetsPath;
	protected String resultPath;
	
	
	public LineRankITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		sourcesPath = createTempFile("sourceIncidence.txt", SOURCE_INCIDENCE);
		targetsPath = createTempFile("targetIncidence.txt", TARGET_INCIDENCE);
		resultPath = getTempFilePath("results");
	}

	@Override
	protected Plan getTestJob() {
		LineRank lr = new LineRank();
		
		Plan plan = lr.getScalaPlan(
			config.getInteger("NumSubtasks", 1), 
			sourcesPath,
			targetsPath,
			9,
			resultPath);
		return plan;
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config1 = new Configuration();
		config1.setInteger("NumSubtasks", 4);
		config1.setInteger("NumIterations", 5);
		return toParameterList(config1);
	}
}
