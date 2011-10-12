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
import eu.stratosphere.pact.common.io.DelimitedOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;


public class EnumTrianglesIBM implements PlanAssembler, PlanAssemblerDescription
{
	// --------------------------------------------------------------------------------------------
	//                              Serialization / Deserialization
	// --------------------------------------------------------------------------------------------

	public static class EdgeInFormat extends DelimitedInputFormat
	{
		private final PactInteger i1 = new PactInteger();
		private final PactInteger i2 = new PactInteger();
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.io.DelimitedInputFormat#readRecord(eu.stratosphere.pact.common.type.PactRecord, byte[], int)
		 */
		@Override
		public boolean readRecord(PactRecord target, byte[] bytes, int numBytes)
		{
			int first = 0, second = 0;
			
			int pos = 0;
			while (pos < numBytes && bytes[pos] != ',') {
				first = first * 10 + (bytes[pos++] - '0');
			}
			pos += 1;// skip the comma
			while (pos < numBytes) {
				second = second * 10 + (bytes[pos++] - '0');
			}
			
			if (first <= 0 || second <= 0 || second <= first)
				return false;
			
			i1.setValue(first);
			i2.setValue(second);
			target.setField(0, i1);
			target.setField(1, i2);
			return true;
		}
	}

	public static class EdgeOutFormat extends DelimitedOutputFormat
	{
		private final StringBuilder line = new StringBuilder();

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.io.DelimitedOutputFormat#serializeRecord(eu.stratosphere.pact.common.type.PactRecord, byte[])
		 */
		@Override
		public int serializeRecord(PactRecord rec, byte[] target) throws Exception
		{
			final int e1 = rec.getField(0, PactInteger.class).getValue();
			final int e2 = rec.getField(1, PactInteger.class).getValue();
			final int e3 = rec.getField(2, PactInteger.class).getValue();
			
			this.line.setLength(0);
			this.line.append(e1);
			this.line.append(',');
			this.line.append(e2);
			this.line.append(',');
			this.line.append(e3);
			this.line.append('\n');
			
			if (target.length >= line.length()) {
				for (int i = 0; i < line.length(); i++) {
					target[i] = (byte) line.charAt(i);
				}
				return line.length();
			}
			else {
				return -line.length();
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                  Vertex Degree Computation
	// --------------------------------------------------------------------------------------------
	
	public static class ProjectEdge extends MapStub
	{
		private final PactRecord copy = new PactRecord();

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MapStub#map(eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void map(PactRecord record, Collector out) throws Exception
		{
			this.copy.setField(0, record.getField(1, PactInteger.class));
			this.copy.setField(1, record.getField(0, PactInteger.class));
			
			out.collect(this.copy);
			out.collect(record);
		}
	}
	
	public static class CountEdges extends ReduceStub
	{
		private final PactRecord result = new PactRecord();
		
		private final PactInteger firstVertex = new PactInteger();
		private final PactInteger secondVertex = new PactInteger();
		private final PactInteger firstCount = new PactInteger();
		private final PactInteger secondCount = new PactInteger();
		
		private int[] vals = new int[1024];
		private int len = 0;

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.ReduceStub#reduce(java.util.Iterator, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void reduce(Iterator<PactRecord> records, Collector out) throws Exception
		{
			int key = -1;
			
			// collect all values
			this.len = 0;
			while (records.hasNext()) {
				final PactRecord rec = records.next();
				final int id = rec.getField(1, PactInteger.class).getValue();
				if (key == -1) {
					key = rec.getField(0, PactInteger.class).getValue();
				}
				
				if (this.len >= this.vals.length) {
					int[] na = new int[this.vals.length * 2];
					System.arraycopy(this.vals, 0, na, 0, this.vals.length);
					this.vals = na;
				}
				this.vals[this.len++] = id;
			}
			
			for (int i = 0; i < this.len; i++)
			{
				final int e2 = this.vals[i];
				if (key <= e2) {
					firstVertex.setValue(key);
					secondVertex.setValue(e2);
					firstCount.setValue(len);
					secondCount.setValue(0);
				} else {
					firstVertex.setValue(e2);
					secondVertex.setValue(key);
					firstCount.setValue(0);
					secondCount.setValue(len);
				}
				this.result.setField(0, firstVertex);
				this.result.setField(1, secondVertex);
				this.result.setField(2, firstCount);
				this.result.setField(3, secondCount);
				out.collect(result);
			}
		}
	}
	
	public static class JoinCountsAndUniquify extends ReduceStub
	{
		private final PactInteger count1 = new PactInteger();
		private final PactInteger count2 = new PactInteger();

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.ReduceStub#reduce(java.util.Iterator, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void reduce(Iterator<PactRecord> records, Collector out) throws Exception
		{
			PactRecord rec = null;
			int c1 = 0, c2 = 0;
			
			while (records.hasNext()) {
				rec = records.next();
				
				int f1 = rec.getField(2, PactInteger.class).getValue();
				int f2 = rec.getField(3, PactInteger.class).getValue();
				
				if (c1 == 0 && f1 != 0)
					c1 += f1;
				
				if (c2 == 0 && f2 != 0)
					c2 += f2;
			}
			
			count1.setValue(c1);
			count2.setValue(c2);
			rec.setField(2, count1);
			rec.setField(3, count2);
			out.collect(rec);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                  Triangle Enumeration
	// --------------------------------------------------------------------------------------------
	
	public static class ProjectOutCounts extends MapStub
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
	
	public static class ProjectToLowerDegreeVertex extends MapStub
	{
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MapStub#map(eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void map(PactRecord record, Collector out) throws Exception
		{
			if (record.getField(2, PactInteger.class).getValue() > record.getField(3, PactInteger.class).getValue()) {
				PactInteger first = record.getField(1, PactInteger.class);
				PactInteger second = record.getField(0, PactInteger.class);
				record.setField(0, first);
				record.setField(1, second);
			}
			
			record.setNumFields(2);
			out.collect(record);
		}
	}

	public static class BuildTriads extends ReduceStub
	{
		private final PactInteger firstVertex = new PactInteger();
		private final PactInteger secondVertex = new PactInteger();
		
		private int[] edgeCache = new int[1024];
		private int len = 0;

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.ReduceStub#reduce(java.util.Iterator, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void reduce(Iterator<PactRecord> records, Collector out) throws Exception
		{
			this.len = 0;
			
			PactRecord rec = null;
			while (records.hasNext())
			{
				rec = records.next();
				final int e1 = rec.getField(1, PactInteger.class).getValue();
				
				for (int i = 0; i < this.len; i++)
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
				
				if (this.len >= this.edgeCache.length) {
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
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String edgeInput = "hdfs://cloud-7.dima.tu-berlin.de:40010/graph/ibm_interactions.csv";
		String output    = "hdfs://cloud-7.dima.tu-berlin.de:40010/graph/result";

		FileDataSource edges = new FileDataSource(EdgeInFormat.class, edgeInput, "Input Edges");
		edges.setParameter(DelimitedInputFormat.RECORD_DELIMITER, "\n");
		edges.setDegreeOfParallelism(noSubTasks);

		// =========================== Vertex Degree ============================
		
		MapContract projectEdge = new MapContract(ProjectEdge.class, edges, "Project Edge");
		projectEdge.setDegreeOfParallelism(noSubTasks);
		
		ReduceContract edgeCounter = new ReduceContract(CountEdges.class, 0, PactInteger.class, projectEdge, "Count Adjacent Edges");
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

		FileDataSink triangles = new FileDataSink(EdgeOutFormat.class, output, closeTriads, "Triangles");
		triangles.setDegreeOfParallelism(noSubTasks);

		return new Plan(triangles, "Enumerate Triangles");
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.PlanAssemblerDescription#getDescription()
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks]";
	}
}
