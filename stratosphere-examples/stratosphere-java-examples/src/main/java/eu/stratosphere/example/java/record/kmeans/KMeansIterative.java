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

package eu.stratosphere.example.java.record.kmeans;


import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.BulkIteration;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.example.java.record.kmeans.KMeansSingleStep.PointBuilder;
import eu.stratosphere.example.java.record.kmeans.KMeansSingleStep.RecomputeClusterCenter;
import eu.stratosphere.example.java.record.kmeans.KMeansSingleStep.SelectNearestCenter;
import eu.stratosphere.example.java.record.kmeans.udfs.PointOutFormat;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;


public class KMeansIterative implements Program, ProgramDescription {
	
	private static final long serialVersionUID = 1L;

	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataPointInput = (args.length > 1 ? args[1] : "");
		String clusterInput = (args.length > 2 ? args[2] : "");
		String output = (args.length > 3 ? args[3] : "");
		int numIterations = (args.length > 4 ? Integer.parseInt(args[4]) : 2);

		// create DataSourceContract for data point input
		@SuppressWarnings("unchecked")
		FileDataSource pointsSource = new FileDataSource(new CsvInputFormat('|', IntValue.class, DoubleValue.class, DoubleValue.class, DoubleValue.class), dataPointInput, "Data Points");

		// create DataSourceContract for cluster center input
		@SuppressWarnings("unchecked")
		FileDataSource clustersSource = new FileDataSource(new CsvInputFormat('|', IntValue.class, DoubleValue.class, DoubleValue.class, DoubleValue.class), clusterInput, "Centers");
		
		MapOperator dataPoints = MapOperator.builder(new PointBuilder()).name("Build data points").input(pointsSource).build();
		
		MapOperator clusterPoints = MapOperator.builder(new PointBuilder()).name("Build cluster points").input(clustersSource).build();
		
		BulkIteration iter = new BulkIteration("k-means loop");
		iter.setInput(clusterPoints);
		iter.setMaximumNumberOfIterations(numIterations);

		// create CrossOperator for distance computation
		MapOperator findNearestClusterCenters = MapOperator.builder(new SelectNearestCenter())
			.setBroadcastVariable("centers", iter.getPartialSolution())
			.input(dataPoints)
			.name("Find Nearest Centers")
			.build();

		// create ReduceOperator for computing new cluster positions
		ReduceOperator recomputeClusterCenter = ReduceOperator.builder(new RecomputeClusterCenter(), IntValue.class, 0)
			.input(findNearestClusterCenters)
			.name("Recompute Center Positions")
			.build();
		
		iter.setNextPartialSolution(recomputeClusterCenter);

		// create DataSinkContract for writing the new cluster positions
		FileDataSink newClusterPoints = new FileDataSink(new PointOutFormat(), output, iter, "New Center Positions");

		// return the PACT plan
		Plan plan = new Plan(newClusterPoints, "KMeans Iteration");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: <numSubStasks> <dataPoints> <clusterCenters> <output> <numIterations>";
	}
}
