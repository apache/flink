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
import eu.stratosphere.test.testdata.KMeansData;
import eu.stratosphere.test.util.RecordAPITestBase;

public class IterativeKMeansITCase extends RecordAPITestBase {

	static {
		Locale.setDefault(Locale.US);
	}
	
	protected String pointsPath;
	protected String clusterPath;
	protected String resultPath;
	
	
	@Override
	protected void preSubmit() throws Exception {
		pointsPath = createTempFile("datapoints.txt", KMeansData.DATAPOINTS);
		clusterPath = createTempFile("initial_centers.txt", KMeansData.INITIAL_CENTERS);
		resultPath = getTempDirPath("resulting_centers");
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(KMeansData.CENTERS_AFTER_20_ITERATIONS_SINGLE_DIGIT, resultPath);
	}
	

	@Override
	protected Plan getTestJob() {
		KMeans kmi = new KMeans();
		return kmi.getScalaPlan(4, pointsPath, clusterPath, resultPath, 20);
	}
}
