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

package eu.stratosphere.test.recordJobs.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.DelimitedInputFormat;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 * Implementation of the triangle enumeration example Pact program.
 * The program expects a file with RDF triples (in XML serialization) as input. Triples must be separated by linebrakes.
 * 
 * The program filters for foaf:knows predicates to identify relationships between two entities (typically persons).
 * Relationships are interpreted as edges in a social graph. Then the program enumerates all triangles which are build 
 * by edges in that graph. 
 * 
 * Usually, triangle enumeration is used as a pre-processing step to identify highly connected subgraphs.
 * The algorithm was published as MapReduce job by J. Cohen in "Graph Twiddling in a MapReduce World".
 * The Pact version was described in "MapReduce and PACT - Comparing Data Parallel Programming Models" (BTW 2011).
 */
public class EnumTrianglesRdfFoaf implements Program, ProgramDescription {

	private static final long serialVersionUID = 1L;

	/**
	 * Reads RDF triples and filters on the foaf:knows RDF predicate.
	 * The foaf:knows RDF predicate indicates that the RDF subject and object (typically of type foaf:person) know each
	 * other.
	 * Therefore, knowing connections between people are extracted and handles as graph edges.
	 * The EdgeInFormat filters all rdf triples with foaf:knows predicates. The subjects and objects URLs are
	 * compared.
	 * The lexicographically smaller URL is set as the first field of the output record, the greater one as the second field.
	 */
	public static class EdgeInFormat extends DelimitedInputFormat {
		private static final long serialVersionUID = 1L;

		private final StringValue rdfSubj = new StringValue();
		private final StringValue rdfPred = new StringValue();
		private final StringValue rdfObj = new StringValue();
		
		@Override
		public Record readRecord(Record target, byte[] bytes, int offset, int numBytes) {
			final int limit = offset + numBytes;
			int startPos = offset;
			
			// read RDF subject
			startPos = parseVarLengthEncapsulatedStringField(bytes, startPos, limit, ' ', rdfSubj, '"');
			if (startPos < 0) {
				// invalid record, exit
				return null;
			}
			// read RDF predicate
			startPos = parseVarLengthEncapsulatedStringField(bytes, startPos, limit, ' ', rdfPred, '"');
			if (startPos < 0 || !rdfPred.getValue().equals("<http://xmlns.com/foaf/0.1/knows>")) {
				// invalid record or predicate is not a foaf-knows predicate, exit
				return null;
			}
			// read RDF object
			startPos = parseVarLengthEncapsulatedStringField(bytes, startPos, limit, ' ', rdfObj, '"');
			if (startPos < 0) {
				// invalid record, exit
				return null;
			}

			// compare RDF subject and object
			if (rdfSubj.compareTo(rdfObj) <= 0) {
				// subject is smaller, subject becomes first attribute, object second
				target.setField(0, rdfSubj);
				target.setField(1, rdfObj);
			} else {
				// object is smaller, object becomes first attribute, subject second
				target.setField(0, rdfObj);
				target.setField(1, rdfSubj);
			}

			return target;	
		}
		
		/*
		 * Utility method to efficiently parse encapsulated, variable length strings 
		 */
		private int parseVarLengthEncapsulatedStringField(byte[] bytes, int startPos, int limit, char delim, StringValue field, char encaps) {
			
			boolean isEncaps = false;
			
			// check whether string is encapsulated
			if (bytes[startPos] == encaps) {
				isEncaps = true;
			}
			
			if (isEncaps) {
				// string is encapsulated
				for (int i = startPos; i < limit; i++) {
					if (bytes[i] == encaps) {
						if (bytes[i+1] == delim) {
							field.setValueAscii(bytes, startPos, i-startPos+1);
							return i+2;
						}
					}
				}
				return -1;
			} else {
				// string is not encapsulated
				int i;
				for (i = startPos; i < limit; i++) {
					if (bytes[i] == delim) {
						field.setValueAscii(bytes, startPos, i-startPos);
						return i+1;
					}
				}
				if (i == limit) {
					field.setValueAscii(bytes, startPos, i-startPos);
					return i+1;
				} else {
					return -1;
				}
			}
		}
	}

	/**
	 * Builds triads (open triangle) from all two edges that share a vertex.
	 * The common vertex is 
	 */
	@ConstantFields(0)
	public static class BuildTriads extends ReduceFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		// list of non-matching vertices
		private final ArrayList<StringValue> otherVertices = new ArrayList<StringValue>(32);
		
		// matching vertex
		private final StringValue matchVertex = new StringValue();
		
		// mutable output record
		private final Record result = new Record();
		
		// initialize list of non-matching vertices for one vertex
		public BuildTriads() {
			this.otherVertices.add(new StringValue());
		}

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
			// read the first edge
			final Record rec = records.next();
			// read the matching vertex
			rec.getFieldInto(0, this.matchVertex);
			// read the non-matching vertex and add it to the list
			rec.getFieldInto(1, this.otherVertices.get(0));
			
			// set the matching vertex in the output record
			this.result.setField(0, this.matchVertex);
			
			int numEdges = 1;
			// while there are more edges
			while (records.hasNext()) {

				// read the next edge
				final Record next = records.next();
				
				final StringValue myVertex;
				// obtain an object to store the non-matching vertex
				if (numEdges >= this.otherVertices.size()) {
					// we need an additional vertex object
					// create the object
					myVertex = new StringValue();
					// and put it in the list
					this.otherVertices.add(myVertex);
				} else {
					// we reuse a previously created object from the list
					myVertex = this.otherVertices.get(numEdges);
				}
				// read the non-matching vertex into the obtained object
				next.getFieldInto(1, myVertex);
				
				// combine the current edge with all vertices in the non-matching vertex list
				for (int i = 0; i < numEdges; i++) {
					// get the other non-matching vertex
					final StringValue otherVertex = this.otherVertices.get(i);
					// add my and other vertex to the output record depending on their ordering 
					if (otherVertex.compareTo(myVertex) < 0) {
						this.result.setField(1, otherVertex);
						this.result.setField(2, myVertex);
						out.collect(this.result);
					} else {
						next.setField(2, otherVertex);
						out.collect(next);
					}
				}
				
				numEdges++;
			}
		}		
	}

	/**
	 * Matches all missing edges with existing edges from input.
	 * If the missing edge for a triad is found, the triad is transformed to a triangle by adding the missing edge.
	 */
	@ConstantFieldsFirstExcept({})
	public static class CloseTriads extends JoinFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public void join(Record triad, Record missingEdge, Collector<Record> out) throws Exception {
			// emit triangle (already contains missing edge at field 0
			out.collect(triad);
		}
	}

	/**
	 * Assembles the Plan of the triangle enumeration example Pact program.
	 */
	@Override
	public Plan getPlan(String... args) {

		// parse job parameters
		int numSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String edgeInput = (args.length > 1 ? args[1] : "");
		String output    = (args.length > 2 ? args[2] : "");

		FileDataSource edges = new FileDataSource(new EdgeInFormat(), edgeInput, "BTC Edges");
		
		ReduceOperator buildTriads = ReduceOperator.builder(new BuildTriads(), StringValue.class, 0)
			.name("Build Triads")
			.build();

		JoinOperator closeTriads = JoinOperator.builder(new CloseTriads(), StringValue.class, 1, 0)
			.keyField(StringValue.class, 2, 1)
			.name("Close Triads")
			.build();
		closeTriads.setParameter("INPUT_LEFT_SHIP_STRATEGY", "SHIP_REPARTITION_HASH");
		closeTriads.setParameter("INPUT_RIGHT_SHIP_STRATEGY", "SHIP_REPARTITION_HASH");
		closeTriads.setParameter("LOCAL_STRATEGY", "LOCAL_STRATEGY_HASH_BUILD_SECOND");

		FileDataSink triangles = new FileDataSink(new CsvOutputFormat(), output, "Output");
		CsvOutputFormat.configureRecordFormat(triangles)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(StringValue.class, 0)
			.field(StringValue.class, 1)
			.field(StringValue.class, 2);

		triangles.setInput(closeTriads);
		closeTriads.setSecondInput(edges);
		closeTriads.setFirstInput(buildTriads);
		buildTriads.setInput(edges);

		Plan plan = new Plan(triangles, "Enumerate Triangles");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.PlanAssemblerDescription#getDescription()
	 */
	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks] [inputRDFTriples] [outputTriangles]";
	}
}
