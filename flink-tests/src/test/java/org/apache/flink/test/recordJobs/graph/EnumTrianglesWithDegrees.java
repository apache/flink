/*
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

package org.apache.flink.test.recordJobs.graph;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.JoinOperator;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.test.recordJobs.graph.ComputeEdgeDegrees.CountEdges;
import org.apache.flink.test.recordJobs.graph.ComputeEdgeDegrees.JoinCountsAndUniquify;
import org.apache.flink.test.recordJobs.graph.ComputeEdgeDegrees.ProjectEdge;
import org.apache.flink.test.recordJobs.graph.EnumTrianglesOnEdgesWithDegrees.BuildTriads;
import org.apache.flink.test.recordJobs.graph.EnumTrianglesOnEdgesWithDegrees.CloseTriads;
import org.apache.flink.test.recordJobs.graph.EnumTrianglesOnEdgesWithDegrees.ProjectOutCounts;
import org.apache.flink.test.recordJobs.graph.EnumTrianglesOnEdgesWithDegrees.ProjectToLowerDegreeVertex;
import org.apache.flink.test.recordJobs.graph.triangleEnumUtil.EdgeInputFormat;
import org.apache.flink.test.recordJobs.graph.triangleEnumUtil.TriangleOutputFormat;
import org.apache.flink.types.IntValue;

/**
 * An implementation of the triangle enumeration, which includes the pre-processing step
 * to compute the degrees of the vertices and to select the lower-degree vertex for the
 * enumeration of open triads.
 */
@SuppressWarnings("deprecation")
public class EnumTrianglesWithDegrees implements Program, ProgramDescription {
	
	private static final long serialVersionUID = 1L;

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
