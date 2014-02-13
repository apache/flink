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

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.BulkIteration;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.java.record.operators.CrossOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.example.java.record.kmeans.udfs.ComputeDistance;
import eu.stratosphere.example.java.record.kmeans.udfs.FindNearestCenter;
import eu.stratosphere.example.java.record.kmeans.udfs.PointInFormat;
import eu.stratosphere.example.java.record.kmeans.udfs.PointOutFormat;
import eu.stratosphere.example.java.record.kmeans.udfs.RecomputeClusterCenter;
import eu.stratosphere.types.IntValue;


public class KMeansIterative implements Program, ProgramDescription {

	private static final long serialVersionUID = 1L;

	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String dataPointInput = (args.length > 1 ? args[1] : "");
		final String clusterInput = (args.length > 2 ? args[2] : "");
		final String output = (args.length > 3 ? args[3] : "");
		final int numIterations = (args.length > 4 ? Integer.parseInt(args[4]) : 1);

		// create DataSourceContract for cluster center input
		FileDataSource initialClusterPoints = new FileDataSource(new PointInFormat(), clusterInput, "Centers");
		initialClusterPoints.setDegreeOfParallelism(1);
		
		BulkIteration iteration = new BulkIteration("K-Means Loop");
		iteration.setInput(initialClusterPoints);
		iteration.setMaximumNumberOfIterations(numIterations);
		
		// create DataSourceContract for data point input
		FileDataSource dataPoints = new FileDataSource(new PointInFormat(), dataPointInput, "Data Points");

		// create CrossOperator for distance computation
		CrossOperator computeDistance = CrossOperator.builder(new ComputeDistance())
				.input1(dataPoints)
				.input2(iteration.getPartialSolution())
				.name("Compute Distances")
				.build();

		// create ReduceOperator for finding the nearest cluster centers
		ReduceOperator findNearestClusterCenters = ReduceOperator.builder(new FindNearestCenter(), IntValue.class, 0)
				.input(computeDistance)
				.name("Find Nearest Centers")
				.build();

		// create ReduceOperator for computing new cluster positions
		ReduceOperator recomputeClusterCenter = ReduceOperator.builder(new RecomputeClusterCenter(), IntValue.class, 0)
				.input(findNearestClusterCenters)
				.name("Recompute Center Positions")
				.build();
		iteration.setNextPartialSolution(recomputeClusterCenter);
		
		// create DataSourceContract for data point input
		FileDataSource dataPoints2 = new FileDataSource(new PointInFormat(), dataPointInput, "Data Points 2");
		
		// compute distance of points to final clusters 
		CrossOperator computeFinalDistance = CrossOperator.builder(new ComputeDistance())
				.input1(dataPoints2)
				.input2(iteration)
				.name("Compute Final Distances")
				.build();

		// find nearest final cluster for point
		ReduceOperator findNearestFinalCluster = ReduceOperator.builder(new FindNearestCenter(), IntValue.class, 0)
				.input(computeFinalDistance)
				.name("Find Nearest Final Centers")
				.build();

		// create DataSinkContract for writing the new cluster positions
		FileDataSink finalClusters = new FileDataSink(new PointOutFormat(), output+"/centers", iteration, "Cluster Positions");

		// write assigned clusters
		FileDataSink clusterAssignments = new FileDataSink(new PointOutFormat(), output+"/points", findNearestFinalCluster, "Cluster Assignments");
		
		List<GenericDataSink> sinks = new ArrayList<GenericDataSink>();
		sinks.add(finalClusters);
		sinks.add(clusterAssignments);
		
		// return the PACT plan
		Plan plan = new Plan(sinks, "Iterative KMeans");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: <numSubStasks> <dataPoints> <clusterCenters> <output> <numIterations>";
	}
	
	public static void main(String[] args) throws Exception {
		KMeansIterative kmi = new KMeansIterative();
		
		if (args.length < 5) {
			System.err.println(kmi.getDescription());
			System.exit(1);
		}
		
		Plan plan = kmi.getPlan(args);
		
		// This will execute the kMeans clustering job embedded in a local context.
		LocalExecutor.execute(plan);

	}
}
