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

package eu.stratosphere.pact.example.kmeans;


import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.example.kmeans.udfs.ComputeDistance;
import eu.stratosphere.pact.example.kmeans.udfs.FindNearestCenter;
import eu.stratosphere.pact.example.kmeans.udfs.PointInFormat;
import eu.stratosphere.pact.example.kmeans.udfs.PointOutFormat;
import eu.stratosphere.pact.example.kmeans.udfs.RecomputeClusterCenter;

/**
 * The K-Means cluster algorithm is well-known (see
 * http://en.wikipedia.org/wiki/K-means_clustering). KMeansIteration is a PACT
 * program that computes a single iteration of the k-means algorithm. The job
 * has two inputs, a set of data points and a set of cluster centers. A Cross
 * PACT is used to compute all distances from all centers to all points. A
 * following Reduce PACT assigns each data point to the cluster center that is
 * next to it. Finally, a second Reduce PACT compute the new locations of all
 * cluster centers.
 */
public class KMeansSingleStep implements PlanAssembler, PlanAssemblerDescription {
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataPointInput = (args.length > 1 ? args[1] : "");
		String clusterInput = (args.length > 2 ? args[2] : "");
		String output = (args.length > 3 ? args[3] : "");

		// create DataSourceContract for data point input
		FileDataSource dataPoints = new FileDataSource(new PointInFormat(), dataPointInput, "Data Points");
		dataPoints.getCompilerHints().addUniqueField(0);

		// create DataSourceContract for cluster center input
		FileDataSource clusterPoints = new FileDataSource(new PointInFormat(), clusterInput, "Centers");
		clusterPoints.setDegreeOfParallelism(1);
		clusterPoints.getCompilerHints().addUniqueField(0);

		// create CrossContract for distance computation
		CrossContract computeDistance = CrossContract.builder(new ComputeDistance())
			.input1(dataPoints)
			.input2(clusterPoints)
			.name("Compute Distances")
			.build();

		// create ReduceContract for finding the nearest cluster centers
		ReduceContract findNearestClusterCenters = ReduceContract.builder(new FindNearestCenter(), PactInteger.class, 0)
			.input(computeDistance)
			.name("Find Nearest Centers")
			.build();

		// create ReduceContract for computing new cluster positions
		ReduceContract recomputeClusterCenter = ReduceContract.builder(new RecomputeClusterCenter(), PactInteger.class, 0)
			.input(findNearestClusterCenters)
			.name("Recompute Center Positions")
			.build();

		// create DataSinkContract for writing the new cluster positions
		FileDataSink newClusterPoints = new FileDataSink(new PointOutFormat(), output, recomputeClusterCenter, "New Center Positions");

		// return the PACT plan
		Plan plan = new Plan(newClusterPoints, "KMeans Iteration");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks] [dataPoints] [clusterCenters] [output]";
	}
}
