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

package eu.stratosphere.example.record.triangles;

import java.io.Serializable;
import java.util.Iterator;

import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.plan.Plan;
import eu.stratosphere.api.plan.PlanAssembler;
import eu.stratosphere.api.plan.PlanAssemblerDescription;
import eu.stratosphere.api.record.functions.MapStub;
import eu.stratosphere.api.record.functions.MatchStub;
import eu.stratosphere.api.record.functions.ReduceStub;
import eu.stratosphere.api.record.operators.MapOperator;
import eu.stratosphere.api.record.operators.JoinOperator;
import eu.stratosphere.api.record.operators.ReduceOperator;
import eu.stratosphere.example.record.triangles.io.EdgeWithDegreesInputFormat;
import eu.stratosphere.example.record.triangles.io.TriangleOutputFormat;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;


/**
 * An implementation of the triangle enumeration, which expects its input to
 * encode the degrees of the vertices. The algorithm selects the lower-degree vertex for the
 * enumeration of open triads.
 */
public class EnumTrianglesOnEdgesWithDegrees implements PlanAssembler, PlanAssemblerDescription {

	// --------------------------------------------------------------------------------------------
	//                                  Triangle Enumeration
	// --------------------------------------------------------------------------------------------
	
	public static final class ProjectOutCounts extends MapStub implements Serializable {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			record.setNumFields(2);
			out.collect(record);
		}
	}
	
	public static final class ProjectToLowerDegreeVertex extends MapStub implements Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			final int d1 = record.getField(2, PactInteger.class).getValue();
			final int d2 = record.getField(3, PactInteger.class).getValue();
			if (d1 > d2) {
				PactInteger first = record.getField(1, PactInteger.class);
				PactInteger second = record.getField(0, PactInteger.class);
				record.setField(0, first);
				record.setField(1, second);
			}
			record.setNumFields(2);
			out.collect(record);
		}
	}

	public static final class BuildTriads extends ReduceStub implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final PactInteger firstVertex = new PactInteger();
		private final PactInteger secondVertex = new PactInteger();
		
		private int[] edgeCache = new int[1024];

		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
			int len = 0;
			
			PactRecord rec = null;
			while (records.hasNext()) {
				rec = records.next();
				final int e1 = rec.getField(1, PactInteger.class).getValue();
				
				for (int i = 0; i < len; i++) {
					final int e2 = this.edgeCache[i];
					
					if (e1 <= e2) {
						firstVertex.setValue(e1);
						secondVertex.setValue(e2);
					} else {
						firstVertex.setValue(e2);
						secondVertex.setValue(e1);
					}
					
					rec.setField(1, firstVertex);
					rec.setField(2, secondVertex);
					out.collect(rec);
				}
				
				if (len >= this.edgeCache.length) {
					int[] na = new int[this.edgeCache.length * 2];
					System.arraycopy(this.edgeCache, 0, na, 0, this.edgeCache.length);
					this.edgeCache = na;
				}
				this.edgeCache[len++] = e1;
			}
		}
	}

	public static class CloseTriads extends MatchStub implements Serializable {
		private static final long serialVersionUID = 1L;
		@Override
		public void match(PactRecord triangle, PactRecord missingEdge, Collector<PactRecord> out) throws Exception {
			out.collect(triangle);
		}
	}

	/**
	 * Assembles the Plan of the triangle enumeration example Pact program.
	 */
	@Override
	public Plan getPlan(String... args) {

		// parse job parameters
		int numSubTasks   = args.length > 0 ? Integer.parseInt(args[0]) : 1;
		String edgeInput = args.length > 1 ? args[1] : "";
		String output    = args.length > 2 ? args[2] : "";

		FileDataSource edges = new FileDataSource(new EdgeWithDegreesInputFormat(), edgeInput, "Input Edges with Degrees");
		edges.setParameter(EdgeWithDegreesInputFormat.VERTEX_DELIMITER_CHAR, '|');
		edges.setParameter(EdgeWithDegreesInputFormat.DEGREE_DELIMITER_CHAR, ',');

		// =========================== Triangle Enumeration ============================
		
		MapOperator toLowerDegreeEdge = MapOperator.builder(new ProjectToLowerDegreeVertex())
				.input(edges)
				.name("Select lower-degree Edge")
				.build();
		
		MapOperator projectOutCounts = MapOperator.builder(new ProjectOutCounts())
				.input(edges)
				.name("Project to vertex Ids only")
				.build();

		ReduceOperator buildTriads = ReduceOperator.builder(new BuildTriads(), PactInteger.class, 0)
				.input(toLowerDegreeEdge)
				.name("Build Triads")
				.build();

		JoinOperator closeTriads = JoinOperator.builder(new CloseTriads(), PactInteger.class, 1, 0)
				.keyField(PactInteger.class, 2, 1)
				.input1(buildTriads)
				.input2(projectOutCounts)
				.name("Close Triads")
				.build();
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
