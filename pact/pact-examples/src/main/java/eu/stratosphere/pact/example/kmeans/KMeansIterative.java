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
import eu.stratosphere.pact.generic.contract.BulkIteration;

/**
 *
 */
public class KMeansIterative implements PlanAssembler, PlanAssemblerDescription {
	
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

		// create CrossContract for distance computation
		CrossContract computeDistance = CrossContract.builder(new ComputeDistance())
				.input1(dataPoints)
				.input2(iteration.getPartialSolution())
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
		iteration.setNextPartialSolution(recomputeClusterCenter);

		// create DataSinkContract for writing the new cluster positions
		FileDataSink finalResult = new FileDataSink(new PointOutFormat(), output, iteration, "New Center Positions");

		// return the PACT plan
		Plan plan = new Plan(finalResult, "Iterative KMeans");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: <numSubStasks> <dataPoints> <clusterCenters> <output> <numIterations>";
	}
}
