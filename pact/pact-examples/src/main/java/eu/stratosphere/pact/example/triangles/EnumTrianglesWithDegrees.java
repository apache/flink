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

package eu.stratosphere.pact.example.triangles;

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
import eu.stratosphere.pact.example.triangles.ComputeEdgeDegrees.CountEdges;
import eu.stratosphere.pact.example.triangles.ComputeEdgeDegrees.JoinCountsAndUniquify;
import eu.stratosphere.pact.example.triangles.ComputeEdgeDegrees.ProjectEdge;
import eu.stratosphere.pact.example.triangles.EnumTrianglesOnEdgesWithDegrees.BuildTriads;
import eu.stratosphere.pact.example.triangles.EnumTrianglesOnEdgesWithDegrees.CloseTriads;
import eu.stratosphere.pact.example.triangles.EnumTrianglesOnEdgesWithDegrees.ProjectOutCounts;
import eu.stratosphere.pact.example.triangles.EnumTrianglesOnEdgesWithDegrees.ProjectToLowerDegreeVertex;
import eu.stratosphere.pact.example.triangles.io.EdgeInputFormat;
import eu.stratosphere.pact.example.triangles.io.TriangleOutputFormat;

/**
 * An implementation of the triangle enumeration, which includes the pre-processing step
 * to compute the degrees of the vertices and to select the lower-degree vertex for the
 * enumeration of open triads.
 */
public class EnumTrianglesWithDegrees implements PlanAssembler, PlanAssemblerDescription {
	
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		final int numSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String edgeInput = args.length > 1 ? args[1] : "";
		final String output    = args.length > 2 ? args[2] : "";
		final char delimiter   = args.length > 3 ? (char) Integer.parseInt(args[3]) : ',';
		

		FileDataSource edges = new FileDataSource(new EdgeInputFormat(), edgeInput, "Input Edges");
		edges.setParameter(DelimitedInputFormat.RECORD_DELIMITER, "\n");
		edges.setParameter(EdgeInputFormat.ID_DELIMITER_CHAR, delimiter);

		// =========================== Vertex Degree ============================
		
		MapContract projectEdge = MapContract.builder(new ProjectEdge())
				.input(edges).name("Project Edge").build();
		
		ReduceContract edgeCounter = ReduceContract.builder(new CountEdges(), PactInteger.class, 0)
				.input(projectEdge).name("Count Edges for Vertex").build();
		
		ReduceContract countJoiner = ReduceContract.builder(new JoinCountsAndUniquify(), PactInteger.class, 0)
				.keyField(PactInteger.class, 1)
				.input(edgeCounter).name("Join Counts").build();
		
		
		// =========================== Triangle Enumeration ============================
		
		MapContract toLowerDegreeEdge = MapContract.builder(new ProjectToLowerDegreeVertex())
				.input(countJoiner).name("Select lower-degree Edge").build();
		
		MapContract projectOutCounts = MapContract.builder(new ProjectOutCounts())
				.input(countJoiner).name("Project out Counts").build();

		ReduceContract buildTriads = ReduceContract.builder(new BuildTriads(), PactInteger.class, 0)
				.input(toLowerDegreeEdge).name("Build Triads").build();

		MatchContract closeTriads = MatchContract.builder(new CloseTriads(), PactInteger.class, 1, 0)
				.keyField(PactInteger.class, 2, 1)
				.input1(buildTriads).input2(projectOutCounts)
				.name("Close Triads").build();
		closeTriads.setParameter("INPUT_SHIP_STRATEGY", "SHIP_REPARTITION_HASH");
		closeTriads.setParameter("LOCAL_STRATEGY", "LOCAL_STRATEGY_HASH_BUILD_SECOND");

		FileDataSink triangles = new FileDataSink(new TriangleOutputFormat(), output, closeTriads, "Triangles");

		Plan p = new Plan(triangles, "Enumerate Triangles");
		p.setDefaultParallelism(numSubTasks);
		return p;
	}

	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [input file] [output file]";
	}
}
