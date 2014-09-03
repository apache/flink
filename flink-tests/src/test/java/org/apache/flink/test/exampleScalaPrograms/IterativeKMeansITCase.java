/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.test.exampleScalaPrograms;

import java.util.Locale;

import org.apache.flink.api.common.Plan;
import org.apache.flink.examples.scala.datamining.KMeans;
import org.apache.flink.test.testdata.KMeansData;
import org.apache.flink.test.util.RecordAPITestBase;

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
