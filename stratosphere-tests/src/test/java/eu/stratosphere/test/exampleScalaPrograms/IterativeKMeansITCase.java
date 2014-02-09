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

import java.util.Locale;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.examples.scala.datamining.KMeans;
import eu.stratosphere.test.exampleRecordPrograms.KMeansStepITCase;
import eu.stratosphere.test.util.TestBase2;

public class IterativeKMeansITCase extends TestBase2 {

	static {
		Locale.setDefault(Locale.US);
	}
	
	protected String pointsPath;
	protected String clusterPath;
	protected String resultPath;
	
	
	@Override
	protected void preSubmit() throws Exception {
		pointsPath = createTempFile("datapoints.txt", KMeansStepITCase.DATAPOINTS);
		clusterPath = createTempFile("initial_centers.txt", KMeansStepITCase.CLUSTERCENTERS);
		resultPath = getTempDirPath("resulting_centers");
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(CENTERS_AFTER_20_ITERATIONS, resultPath);
	}
	

	@Override
	protected Plan getTestJob() {
		KMeans kmi = new KMeans();
		return kmi.getScalaPlan(4, pointsPath, clusterPath, resultPath, 20);
	}
	
	
	private static final String CENTERS_AFTER_20_ITERATIONS =
			"0|38.3|54.5|19.3|\n" +
			"1|32.1|83.0|50.4|\n" +
			"2|87.5|56.6|20.3|\n" +
			"3|75.4|18.6|67.5|\n" +
			"4|24.9|29.2|77.6|\n" +
			"5|78.7|66.1|70.8|\n" +
			"6|39.5|14.0|18.7|\n";
}
