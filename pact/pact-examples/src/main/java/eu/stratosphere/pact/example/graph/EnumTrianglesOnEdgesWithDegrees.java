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

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.example.graph.io.EdgeWithDegreesInputFormat;
import eu.stratosphere.pact.example.graph.io.TriangleOutputFormat;


public class EnumTrianglesOnEdgesWithDegrees implements PlanAssembler, PlanAssemblerDescription
{
	// --------------------------------------------------------------------------------------------
	//                                  Triangle Enumeration
	// --------------------------------------------------------------------------------------------
	
	public static final class ProjectOutCounts extends MapStub
	{
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MapStub#map(eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void map(PactRecord record, Collector out) throws Exception
		{
			record.setNumFields(2);
			out.collect(record);
		}
	}
	
	public static final class ProjectToLowerDegreeVertex extends MapStub
	{
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MapStub#map(eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void map(PactRecord record, Collector out) throws Exception
		{
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

	public static final class BuildTriads extends ReduceStub
	{
		private final PactInteger firstVertex = new PactInteger();
		private final PactInteger secondVertex = new PactInteger();
		
		private int[] edgeCache = new int[1024];

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.ReduceStub#reduce(java.util.Iterator, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void reduce(Iterator<PactRecord> records, Collector out) throws Exception
		{
			int len = 0;
			
			PactRecord rec = null;
			while (records.hasNext())
			{
				rec = records.next();
				final int e1 = rec.getField(1, PactInteger.class).getValue();
				
				for (int i = 0; i < len; i++)
				{
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

	public static class CloseTriads extends MatchStub
	{
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MatchStub#match(eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void match(PactRecord value1, PactRecord value2, Collector out) throws Exception
		{
			out.collect(value1);
		}
	}

	/**
	 * Assembles the Plan of the triangle enumeration example Pact program.
	 */
	@Override
	public Plan getPlan(String... args) {

		// parse job parameters
		int noSubTasks   = args.length > 0 ? Integer.parseInt(args[0]) : 1;
		String edgeInput = args.length > 1 ? args[1] : "";
		String output    = args.length > 2 ? args[2] : "";

		FileDataSource edges = new FileDataSource(EdgeWithDegreesInputFormat.class, edgeInput, "Input Edges with Degrees");
		edges.setParameter(DelimitedInputFormat.RECORD_DELIMITER, "\n");
		edges.setParameter(EdgeWithDegreesInputFormat.VERTEX_DELIMITER_CHAR, '|');
		edges.setParameter(EdgeWithDegreesInputFormat.DEGREE_DELIMITER_CHAR, ',');
		edges.setDegreeOfParallelism(noSubTasks);

		// =========================== Triangle Enumeration ============================
		
		MapContract toLowerDegreeEdge = new MapContract(ProjectToLowerDegreeVertex.class, edges, "Select lower-degree Edge");
		toLowerDegreeEdge.setDegreeOfParallelism(noSubTasks);
		
		MapContract projectOutCounts = new MapContract(ProjectOutCounts.class, edges, "Project out Counts");
		projectOutCounts.setDegreeOfParallelism(noSubTasks);

		ReduceContract buildTriads = new ReduceContract(BuildTriads.class, PactInteger.class, 0, toLowerDegreeEdge, "Build Triads");
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
