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

package eu.stratosphere.pact.example.graph;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.example.graph.ComputeEdgeDegrees.CountEdges;
import eu.stratosphere.pact.example.graph.ComputeEdgeDegrees.JoinCountsAndUniquify;
import eu.stratosphere.pact.example.graph.ComputeEdgeDegrees.ProjectEdge;
import eu.stratosphere.pact.example.graph.EnumTrianglesOnEdgesWithDegrees.BuildTriads;
import eu.stratosphere.pact.example.graph.EnumTrianglesOnEdgesWithDegrees.CloseTriads;
import eu.stratosphere.pact.example.graph.EnumTrianglesOnEdgesWithDegrees.ProjectOutCounts;
import eu.stratosphere.pact.example.graph.EnumTrianglesOnEdgesWithDegrees.ProjectToLowerDegreeVertex;
import eu.stratosphere.pact.example.graph.io.EdgeInputFormat;
import eu.stratosphere.pact.example.graph.io.TriangleOutputFormat;


public class EnumTriangles implements PlanAssembler, PlanAssemblerDescription
{
	/**
	 * Assembles the Plan of the triangle enumeration example Pact program.
	 */
	@Override
	public Plan getPlan(String... args)
	{
		// parse job parameters
		final int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String edgeInput = args.length > 1 ? args[1] : "";
		final String output    = args.length > 2 ? args[2] : "";
		final char delimiter   = args.length > 3 ? (char) Integer.parseInt(args[3]) : ',';
		

		FileDataSource edges = new FileDataSource(EdgeInputFormat.class, edgeInput, "Input Edges");
		edges.setParameter(DelimitedInputFormat.RECORD_DELIMITER, "\n");
		edges.setParameter(EdgeInputFormat.ID_DELIMITER_CHAR, delimiter);
		edges.setDegreeOfParallelism(noSubTasks);

		// =========================== Vertex Degree ============================
		
		MapContract projectEdge = new MapContract(ProjectEdge.class, edges, "Project Edge");
		projectEdge.setDegreeOfParallelism(noSubTasks);
		
		ReduceContract edgeCounter = new ReduceContract(CountEdges.class, 0, PactInteger.class, projectEdge, "Count Edges for Vertex");
		edgeCounter.setDegreeOfParallelism(noSubTasks);
		
		@SuppressWarnings("unchecked")
		ReduceContract countJoiner = new ReduceContract(JoinCountsAndUniquify.class, new int[] {0, 1}, new Class[] {PactInteger.class, PactInteger.class}, edgeCounter, "Join Counts");
		countJoiner.setDegreeOfParallelism(noSubTasks);
		
		
		// =========================== Triangle Enumeration ============================
		
		MapContract toLowerDegreeEdge = new MapContract(ProjectToLowerDegreeVertex.class, countJoiner, "Select lower-degree Edge");
		toLowerDegreeEdge.setDegreeOfParallelism(noSubTasks);
		
		MapContract projectOutCounts = new MapContract(ProjectOutCounts.class, countJoiner, "Project out Counts");
		projectOutCounts.setDegreeOfParallelism(noSubTasks);

		ReduceContract buildTriads = new ReduceContract(BuildTriads.class, 0, PactInteger.class, toLowerDegreeEdge, "Build Triads");
		buildTriads.setDegreeOfParallelism(noSubTasks);

		@SuppressWarnings("unchecked")
		MatchContract closeTriads = new MatchContract(CloseTriads.class, new Class[] {PactInteger.class, PactInteger.class}, new int[] {1, 2}, new int[] {0, 1}, buildTriads, projectOutCounts, "Close Triads");
		closeTriads.setDegreeOfParallelism(noSubTasks);
		closeTriads.setParameter("LOCAL_STRATEGY", "LOCAL_STRATEGY_HASH_BUILD_SECOND");

		FileDataSink triangles = new FileDataSink(TriangleOutputFormat.class, output, closeTriads, "Triangles");
		triangles.setDegreeOfParallelism(noSubTasks);

		return new Plan(triangles, "Enumerate Triangles");
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.PlanAssemblerDescription#getDescription()
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [input file] [output file]";
	}
}
