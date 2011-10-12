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
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactNull;


public class TriangleEnumerationNoLowerDegree implements PlanAssembler, PlanAssemblerDescription
{


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
			
			if (first <= 0 || second <= 0)
				return false;
			
			if (first <= second) {
				pair.setKey(new PactInteger(first));
				pair.setValue(new PactInteger(second));
			} else {
				pair.setValue(new PactInteger(first));
				pair.setKey(new PactInteger(second));
			}
			return true;
		}
	}

	public static class EdgeOutFormat extends TextOutputFormat<PactInteger, PactLong>
	{
		private final StringBuilder line = new StringBuilder();
		
		@Override
		public byte[] writeLine(KeyValuePair<PactInteger, PactLong> pair)
		{
			final long edge = pair.getValue().getValue();
			
			final int e1 = pair.getKey().getValue();
			final int e2 = (int) (edge >>> 32);
			final int e3 = (int) (edge & 0xffffffff);
			
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

	public static class BuildEdge extends MapStub<PactInteger, PactInteger, PactLong, PactNull>
	{
		@Override
		public void map(PactInteger vertex1, PactInteger vertex2, Collector<PactLong, PactNull> out)
		{
			final int e1 = vertex1.getValue();
			final int e2 = vertex2.getValue();
			
			// we do not connect a node with itself
			if (e1 == e2)
				return;
			
			int larger, smaller;
			if (e1 <= e2) {
				smaller = e1;
				larger = e2;
			} else {
				larger = e1;
				smaller = e2;
			}
			
			long edge = (((long) smaller) << 32) | larger;

			out.collect(new PactLong(edge), PactNull.getInstance());
		}
	}

	public static class BuildTriads extends ReduceStub<PactInteger, PactInteger, PactLong, PactInteger>
	{
		private final ArrayList<PactInteger> egdeCache = new ArrayList<PactInteger>();
		
		@Override
		public void reduce(PactInteger key, Iterator<PactInteger> values, Collector<PactLong, PactInteger> out)
		{
			this.egdeCache.clear();
			
			while (values.hasNext())
			{
				final PactInteger next = values.next();
				final int e1 = next.getValue();
				
				for (int i = 0; i < this.egdeCache.size(); i++)
				{
					final int e2 = this.egdeCache.get(i).getValue();
					
					// we do not connect a node with itself
					if (e1 == e2)
						continue;
					
					int larger, smaller;
					if (e1 <= e2) {
						smaller = e1;
						larger = e2;
					} else {
						larger = e1;
						smaller = e2;
					}
					
					long missingEdge = (((long) smaller) << 32) | larger;
		
					out.collect(new PactLong(missingEdge), key);
				}
				
				this.egdeCache.add(next);
			}
		}
	}

	public static class CloseTriads extends MatchStub<PactLong, PactInteger, PactNull, PactInteger, PactLong>
	{
		@Override
		public void match(PactLong missingEdge, PactInteger openTriadCenter, PactNull empty, Collector<PactInteger, PactLong> out)
		{
			out.collect(openTriadCenter, missingEdge);
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
		edges.getCompilerHints().setAvgNumValuesPerKey(156);

		MapContract<PactInteger, PactInteger, PactLong, PactNull> buildEdge = new MapContract<PactInteger, PactInteger, PactLong, PactNull>(
			BuildEdge.class, "Assign Keys");
		buildEdge.setDegreeOfParallelism(noSubTasks);
		buildEdge.getCompilerHints().setAvgNumValuesPerKey(1);
		buildEdge.getCompilerHints().setAvgRecordsEmittedPerStubCall(1);

		ReduceContract<PactInteger, PactInteger, PactLong, PactInteger> buildTriads = new ReduceContract<PactInteger, PactInteger, PactLong, PactInteger>(
			BuildTriads.class, "Build Triads");
		buildTriads.setDegreeOfParallelism(noSubTasks);
		buildTriads.getCompilerHints().setAvgBytesPerRecord(12);
		buildTriads.getCompilerHints().setAvgRecordsEmittedPerStubCall(5000);

		MatchContract<PactLong, PactInteger, PactNull, PactInteger, PactLong> closeTriads = new MatchContract<PactLong, PactInteger, PactNull, PactInteger, PactLong>(
			CloseTriads.class, "Close Triads");
		closeTriads.setDegreeOfParallelism(noSubTasks);

		FileDataSinkContract<PactInteger, PactLong> triangles = new FileDataSinkContract<PactInteger, PactLong>(
			EdgeOutFormat.class, output, "Triangles");
		triangles.setDegreeOfParallelism(noSubTasks);

		triangles.setInput(closeTriads);
		
		closeTriads.setFirstInput(buildTriads);
		closeTriads.setSecondInput(buildEdge);
		
		buildTriads.setInput(edges);
		buildEdge.setInput(edges);

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