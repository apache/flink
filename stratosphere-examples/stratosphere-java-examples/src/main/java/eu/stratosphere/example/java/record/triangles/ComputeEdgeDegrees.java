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

package eu.stratosphere.example.java.record.triangles;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.example.java.record.triangles.io.EdgeInputFormat;
import eu.stratosphere.example.java.record.triangles.io.EdgeWithDegreesOutputFormat;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;


public class ComputeEdgeDegrees implements Program, ProgramDescription {
	
	private static final long serialVersionUID = 1L;
	
	// --------------------------------------------------------------------------------------------
	//                                  Vertex Degree Computation
	// --------------------------------------------------------------------------------------------

	/**
	 * Duplicates each edge such that: (u,v) becomes (u,v),(v,u)
	 */
	public static final class ProjectEdge extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final Record copy = new Record();

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			this.copy.setField(0, record.getField(1, IntValue.class));
			this.copy.setField(1, record.getField(0, IntValue.class));
			
			out.collect(this.copy);
			out.collect(record);
		}
	}
	
	/**
	 * Creates for all records in the group a record of the form (v1, v2, c1, c2), where
	 * v1 is the lexicographically smaller vertex id and the count for the vertex that
	 * was the key contains the number of edges associated with it. The other count is zero.
	 * This reducer also eliminates duplicate edges. 
	 */
	public static final class CountEdges extends ReduceFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final Record result = new Record();
		
		private final IntValue firstVertex = new IntValue();
		private final IntValue secondVertex = new IntValue();
		private final IntValue firstCount = new IntValue();
		private final IntValue secondCount = new IntValue();
		
		private int[] vals = new int[1024];

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
			int[] vals = this.vals;
			int len = 0;
			int key = -1;
			
			// collect all values
			while (records.hasNext()) {
				final Record rec = records.next();
				final int id = rec.getField(1, IntValue.class).getValue();
				if (key == -1) {
					key = rec.getField(0, IntValue.class).getValue();
				}
				
				if (len >= vals.length) {
					vals = new int[vals.length * 2];
					System.arraycopy(this.vals, 0, vals, 0, this.vals.length);
					this.vals = vals;
				}
				vals[len++] = id;
			}
			
			// sort the values to and uniquify them
			Arrays.sort(vals, 0, len);
			int k = 0;
			for (int curr = -1, i = 0; i < len; i++) {
				int val = vals[i];
				if (val != curr) {
					curr = val;
					vals[k] = vals[i];
					k++;
				}
				else {
					vals[k] = vals[i];
				}
			}
			len = k;
			
			// create such that the vertex with the lower id is always the first
			// both vertices contain a count, which is zero for the non-key vertices
			for (int i = 0; i < len; i++) {
				final int e2 = vals[i];
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
	
	/**
	 * Takes the two separate edge entries (v1, v2, c1, 0) and (v1, v2, 0, c2)
	 * and creates an entry (v1, v2, c1, c2). 
	 */
	public static final class JoinCountsAndUniquify extends ReduceFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final IntValue count1 = new IntValue();
		private final IntValue count2 = new IntValue();

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
			Record rec = null;
			int c1 = 0, c2 = 0;
			int numValues = 0;
			
			while (records.hasNext()) {
				rec = records.next();
				final int f1 = rec.getField(2, IntValue.class).getValue();
				final int f2 = rec.getField(3, IntValue.class).getValue();
				c1 += f1;
				c2 += f2;
				numValues++;
			}
			
			if (numValues != 2 || c1 == 0 || c2 == 0) {
				throw new RuntimeException("JoinCountsAndUniquify Problem: key1=" +
					rec.getField(0, IntValue.class).getValue() + ", key2=" +
					rec.getField(1, IntValue.class).getValue() + 
					"values=" + numValues + ", c1=" + c1 + ", c2=" + c2);
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
	
	/**
	 * Assembles the Plan of the triangle enumeration example Pact program.
	 */
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		final int numSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String edgeInput = args.length > 1 ? args[1] : "";
		final String output    = args.length > 2 ? args[2] : "";
		final char delimiter   = args.length > 3 ? (char) Integer.parseInt(args[3]) : ',';
		

		FileDataSource edges = new FileDataSource(new EdgeInputFormat(), edgeInput, "Input Edges");
		edges.setParameter(EdgeInputFormat.ID_DELIMITER_CHAR, delimiter);
		
		MapOperator projectEdge = MapOperator.builder(new ProjectEdge())
			.input(edges).name("Project Edge").build();
		
		ReduceOperator edgeCounter = ReduceOperator.builder(new CountEdges(), IntValue.class, 0)
			.input(projectEdge).name("Count Edges for Vertex").build();
		
		ReduceOperator countJoiner = ReduceOperator.builder(new JoinCountsAndUniquify())
			.keyField(IntValue.class, 0)
			.keyField(IntValue.class, 1)
			.input(edgeCounter)
			.name("Join Counts")
			.build();

		FileDataSink triangles = new FileDataSink(new EdgeWithDegreesOutputFormat(), output, countJoiner, "Unique Edges With Degrees");

		Plan p = new Plan(triangles, "Normalize Edges and compute Vertex Degrees");
		p.setDefaultParallelism(numSubTasks);
		return p;
	}

	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [input file] [output file] [vertex delimiter]";
	}
}
