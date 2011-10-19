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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;


public class EnumTrianglesIBM2 implements PlanAssembler, PlanAssemblerDescription
{
	
	// --------------------------------------------------------------------------------------------
	//                                      Data Types
	// --------------------------------------------------------------------------------------------
	
	public static final class IntPair implements Key
	{
		private int id1;
		private int id2;
		
		
		public IntPair() {}

		public IntPair(int id1, int id2) {
			this.id1 = id1;
			this.id2 = id2;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(id1);
			out.writeInt(id2);
			
		}
		
		public int getFirst() {
			return id1;
		}
		
		public int getSecond() {
			return id2;
		}
		
		public void setFirst(int val) {
			this.id1 = val;
		}
		
		public void setSecond(int val) {
			this.id2 = val;
		}

		@Override
		public void read(DataInput in) throws IOException {
			id1 = in.readInt();
			id2 = in.readInt();
		}

		@Override
		public int compareTo(Key o) {
			IntPair other = (IntPair) o;
			int diff = id1 - other.id1;
			return diff == 0 ? id2 - other.id2 : diff;
		}

		@Override
		public int hashCode() {
			return id1 ^ Integer.reverse(id2);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof IntPair) {
				IntPair other = (IntPair) obj;
				return id1 == other.id1 && id2 == other.id2;
			}
			else return false;
		}

		@Override
		public String toString() {
			return id1 + ", " + id2;
		}
	}

	// --------------------------------------------------------------------------------------------
	//                              Serialization / Deserialization
	// --------------------------------------------------------------------------------------------

	public static class EdgeInFormat extends TextInputFormat<PactInteger, PactInteger> {

		@Override
		public boolean readLine(KeyValuePair<PactInteger, PactInteger> pair, byte[] line)
		{
			int first = 0, second = 0;
			
			int pos = 0;
			while (pos < line.length && line[pos] != ',') {
				first = first * 10 + (line[pos++] - '0');
			}
			pos += 1;// skip the comma
			while (pos < line.length) {
				second = second * 10 + (line[pos++] - '0');
			}
			
			if (first <= 0 || second <= 0 || second <= first)
				return false;
			
			pair.setKey(new PactInteger(first));
			pair.setValue(new PactInteger(second));
			return true;
		}
	}

	public static class EdgeOutFormat extends TextOutputFormat<PactInteger, IntPair>
	{
		private final StringBuilder line = new StringBuilder();
		
		@Override
		public byte[] writeLine(KeyValuePair<PactInteger, IntPair> pair)
		{			
			final int e1 = pair.getKey().getValue();
			final int e2 = pair.getValue().getFirst();
			final int e3 = pair.getValue().getSecond();
			
			this.line.setLength(0);
			this.line.append(e1);
			this.line.append(',');
			this.line.append(e2);
			this.line.append(',');
			this.line.append(e3);
			this.line.append('\n');
			return line.toString().getBytes();
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                  Vertex Degree Computation
	// --------------------------------------------------------------------------------------------
	
	public static class ProjectEdge extends MapStub<PactInteger, PactInteger, PactInteger, PactInteger>
	{
		@Override
		public void map(PactInteger key, PactInteger value,	Collector<PactInteger, PactInteger> out) {
			out.collect(key, value);
			out.collect(value, key);
		}
	}
	
	public static class CountEdges extends ReduceStub<PactInteger, PactInteger, IntPair, IntPair>
	{
		private final ArrayList<PactInteger> vals = new ArrayList<PactInteger>();
		
		@Override
		public void reduce(PactInteger key, Iterator<PactInteger> values, Collector<IntPair, IntPair> out)
		{
			// collect all values
			this.vals.clear();
			while (values.hasNext()) {
				this.vals.add(values.next());
			}
			
			// produce pairs with the count
			final int e1 = key.getValue();
			final int count = this.vals.size();
			
			for (int i = 0; i < count; i++)
			{
				final int e2 = this.vals.get(i).getValue();
				if (e1 <= e2) {
					IntPair edge = new IntPair(e1, e2);
					IntPair counts = new IntPair(count, 0);
					out.collect(edge, counts);
				} else {
					IntPair edge = new IntPair(e2, e1);
					IntPair counts = new IntPair(0, count);
					out.collect(edge, counts);
				}
			}
		}
	}
	
	public static class JoinCountsAndUniquify extends ReduceStub<IntPair, IntPair, IntPair, IntPair>
	{
		@Override
		public void reduce(IntPair key, Iterator<IntPair> values, Collector<IntPair, IntPair> out)
		{
			int c1 = 0, c2 = 0;
			while (values.hasNext()) {
				IntPair pair = values.next();
				if (c1 == 0 && pair.getFirst() != 0)
					c1 += pair.getFirst();
				
				if (c2 == 0 && pair.getSecond() != 0)
					c2 += pair.getSecond();
			}
			out.collect(key, new IntPair(c1, c2));
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                  Triangle Enumeration
	// --------------------------------------------------------------------------------------------
	
	public static class ProjectOutCounts extends MapStub<IntPair, IntPair, IntPair, PactNull>
	{
		@Override
		public void map(IntPair key, IntPair value, Collector<IntPair, PactNull> out)
		{
			out.collect(key, PactNull.getInstance());
		}
	}
	
	public static class ProjectToLowerDegreeVertex extends MapStub<IntPair, IntPair, PactInteger, PactInteger>
	{
		@Override
		public void map(IntPair key, IntPair value,	Collector<PactInteger, PactInteger> out)
		{
			if (value.getFirst() <= value.getSecond()) {
				out.collect(new PactInteger(key.getFirst()), new PactInteger(key.getSecond()));
			} else {
				out.collect(new PactInteger(key.getSecond()), new PactInteger(key.getFirst()));
			}
		}
	}

	public static class BuildTriads extends ReduceStub<PactInteger, PactInteger, IntPair, PactInteger>
	{
		private final ArrayList<PactInteger> egdeCache = new ArrayList<PactInteger>();
		
		@Override
		public void reduce(PactInteger key, Iterator<PactInteger> values, Collector<IntPair, PactInteger> out)
		{
			this.egdeCache.clear();
			
			while (values.hasNext())
			{
				final PactInteger next = values.next();
				final int e1 = next.getValue();
				
				for (int i = 0; i < this.egdeCache.size(); i++)
				{
					final int e2 = this.egdeCache.get(i).getValue();
					
					int larger, smaller;
					if (e1 <= e2) {
						smaller = e1;
						larger = e2;
					} else {
						larger = e1;
						smaller = e2;
					}
					out.collect(new IntPair(smaller, larger), key);
				}
				
				this.egdeCache.add(next);
			}
		}
	}

	public static class CloseTriads extends MatchStub<IntPair, PactInteger, PactNull, PactInteger, IntPair>
	{
		@Override
		public void match(IntPair missingEdge, PactInteger apex, PactNull empty, Collector<PactInteger, IntPair> out)
		{
			out.collect(apex, missingEdge);
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

		FileDataSourceContract<PactInteger, PactInteger> edges = new FileDataSourceContract<PactInteger, PactInteger>(EdgeInFormat.class,
			edgeInput, "Input Edges");
		edges.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		edges.setDegreeOfParallelism(noSubTasks);
		edges.getCompilerHints().setKeyCardinality(650000);

		// =========================== Vertex Degree ============================
		
		MapContract<PactInteger, PactInteger, PactInteger, PactInteger> projectEdge = new MapContract<PactInteger, PactInteger, PactInteger, PactInteger>(ProjectEdge.class, "Project Edge");
		projectEdge.setDegreeOfParallelism(noSubTasks);
		projectEdge.getCompilerHints().setAvgRecordsEmittedPerStubCall(2.0f);
		
		ReduceContract<PactInteger, PactInteger, IntPair, IntPair> edgeCounter = new ReduceContract<PactInteger, PactInteger, IntPair, IntPair>(CountEdges.class, "Count Adjacent Edges");
		edgeCounter.setDegreeOfParallelism(noSubTasks);
		edgeCounter.getCompilerHints().setAvgBytesPerRecord(16);
		
		ReduceContract<IntPair, IntPair, IntPair, IntPair> countJoiner = new ReduceContract<IntPair, IntPair, IntPair, IntPair>(JoinCountsAndUniquify.class, "Join Counts");
		countJoiner.setDegreeOfParallelism(noSubTasks);
		countJoiner.getCompilerHints().setKeyCardinality(650000);
		countJoiner.getCompilerHints().setAvgBytesPerRecord(16);
		countJoiner.getCompilerHints().setAvgNumValuesPerKey(156);
		
		countJoiner.setInput(edgeCounter);
		edgeCounter.setInput(projectEdge);
		projectEdge.setInput(edges);
		
		
		// =========================== Triangle Enumeration ============================
		
		MapContract<IntPair, IntPair, PactInteger, PactInteger> toLowerDegreeEdge = new MapContract<IntPair, IntPair, PactInteger, PactInteger>(
			ProjectToLowerDegreeVertex.class, "Select lower-degree Edge");
		toLowerDegreeEdge.setDegreeOfParallelism(noSubTasks);
		toLowerDegreeEdge.getCompilerHints().setAvgNumValuesPerKey(156);
		toLowerDegreeEdge.getCompilerHints().setAvgRecordsEmittedPerStubCall(1);
		toLowerDegreeEdge.getCompilerHints().setAvgBytesPerRecord(8);
		
		MapContract<IntPair, IntPair, IntPair, PactNull> projectOutCounts = new MapContract<IntPair, IntPair, IntPair, PactNull>(
				ProjectOutCounts.class, "Project out Counts");
		projectOutCounts.setDegreeOfParallelism(noSubTasks);
		projectOutCounts.getCompilerHints().setAvgNumValuesPerKey(1);
		projectOutCounts.getCompilerHints().setAvgRecordsEmittedPerStubCall(1);
		projectOutCounts.getCompilerHints().setAvgBytesPerRecord(8);

		ReduceContract<PactInteger, PactInteger, IntPair, PactInteger> buildTriads = new ReduceContract<PactInteger, PactInteger, IntPair, PactInteger>(
			BuildTriads.class, "Build Triads");
		buildTriads.setDegreeOfParallelism(noSubTasks);
		buildTriads.getCompilerHints().setAvgBytesPerRecord(12);
		buildTriads.getCompilerHints().setAvgRecordsEmittedPerStubCall(1000);

		MatchContract<IntPair, PactInteger, PactNull, PactInteger, IntPair> closeTriads = new MatchContract<IntPair, PactInteger, PactNull, PactInteger, IntPair>(
			CloseTriads.class, "Close Triads");
		closeTriads.setDegreeOfParallelism(noSubTasks);

		FileDataSinkContract<PactInteger, IntPair> triangles = new FileDataSinkContract<PactInteger, IntPair>(
			EdgeOutFormat.class, output, "Triangles");
		triangles.setDegreeOfParallelism(noSubTasks);

		
		triangles.setInput(closeTriads);
		closeTriads.setFirstInput(buildTriads);
		closeTriads.setSecondInput(projectOutCounts);
		buildTriads.setInput(toLowerDegreeEdge);
		projectOutCounts.setInput(countJoiner);
		toLowerDegreeEdge.setInput(countJoiner);

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
