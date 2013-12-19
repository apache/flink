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

package eu.stratosphere.example.record.triangles;

import eu.stratosphere.api.Plan;
import eu.stratosphere.api.Program;
import eu.stratosphere.api.ProgramDescription;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.record.operators.MapOperator;
import eu.stratosphere.api.record.operators.JoinOperator;
import eu.stratosphere.api.record.operators.ReduceOperator;
import eu.stratosphere.example.record.triangles.ComputeEdgeDegrees.CountEdges;
import eu.stratosphere.example.record.triangles.ComputeEdgeDegrees.JoinCountsAndUniquify;
import eu.stratosphere.example.record.triangles.ComputeEdgeDegrees.ProjectEdge;
import eu.stratosphere.example.record.triangles.EnumTrianglesOnEdgesWithDegrees.BuildTriads;
import eu.stratosphere.example.record.triangles.EnumTrianglesOnEdgesWithDegrees.CloseTriads;
import eu.stratosphere.example.record.triangles.EnumTrianglesOnEdgesWithDegrees.ProjectOutCounts;
import eu.stratosphere.example.record.triangles.EnumTrianglesOnEdgesWithDegrees.ProjectToLowerDegreeVertex;
import eu.stratosphere.example.record.triangles.io.EdgeInputFormat;
import eu.stratosphere.example.record.triangles.io.TriangleOutputFormat;
import eu.stratosphere.types.IntValue;

/**
 * An implementation of the triangle enumeration, which includes the pre-processing step
 * to compute the degrees of the vertices and to select the lower-degree vertex for the
 * enumeration of open triads.
 */
public class EnumTrianglesWithDegrees implements Program, ProgramDescription {
	
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		final int numSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String edgeInput = args.length > 1 ? args[1] : "";
		final String output    = args.length > 2 ? args[2] : "";
		final char delimiter   = args.length > 3 ? (char) Integer.parseInt(args[3]) : ',';
		

		FileDataSource edges = new FileDataSource(new EdgeInputFormat(), edgeInput, "Input Edges");
		edges.setParameter(EdgeInputFormat.ID_DELIMITER_CHAR, delimiter);

		// =========================== Vertex Degree ============================
		
		MapOperator projectEdge = MapOperator.builder(new ProjectEdge())
				.input(edges).name("Project Edge").build();
		
		ReduceOperator edgeCounter = ReduceOperator.builder(new CountEdges(), IntValue.class, 0)
				.input(projectEdge).name("Count Edges for Vertex").build();
		
		ReduceOperator countJoiner = ReduceOperator.builder(new JoinCountsAndUniquify(), IntValue.class, 0)
				.keyField(IntValue.class, 1)
				.input(edgeCounter).name("Join Counts").build();
		
		
		// =========================== Triangle Enumeration ============================
		
		MapOperator toLowerDegreeEdge = MapOperator.builder(new ProjectToLowerDegreeVertex())
				.input(countJoiner).name("Select lower-degree Edge").build();
		
		MapOperator projectOutCounts = MapOperator.builder(new ProjectOutCounts())
				.input(countJoiner).name("Project out Counts").build();

		ReduceOperator buildTriads = ReduceOperator.builder(new BuildTriads(), IntValue.class, 0)
				.input(toLowerDegreeEdge).name("Build Triads").build();

		JoinOperator closeTriads = JoinOperator.builder(new CloseTriads(), IntValue.class, 1, 0)
				.keyField(IntValue.class, 2, 1)
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
