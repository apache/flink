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

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

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
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
 *
 */
public class EnumTriangles implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * Reads RDF triples and filters on the foaf:knows RDF predicate.
	 * The foaf:knows RDF predicate indicates that the RDF subject and object (typically of type foaf:person) know each
	 * other.
	 * Therefore, knowing connections between people are extracted and handles as graph edges.
	 * The EdgeInFormat filters all rdf triples with foaf:knows predicates. The subjects and objects URLs are
	 * compared.
	 * The lexicographically smaller URL is set as the first field of the output record, the greater one as the second field.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
	 */
	public static class EdgeInFormat extends DelimitedInputFormat {

		private final PactString rdfSubj = new PactString();
		private final PactString rdfPred = new PactString();
		private final PactString rdfObj = new PactString();
		
		@Override
		public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes)
		{
			final int limit = offset + numBytes;
			int startPos = offset;
			
			// read RDF subject
			startPos = parseVarLengthEncapsulatedStringField(bytes, startPos, limit, ' ', rdfSubj, '"');
			if (startPos < 0) 
				// invalid record, exit
				return false;
			// read RDF predicate
			startPos = parseVarLengthEncapsulatedStringField(bytes, startPos, limit, ' ', rdfPred, '"');
			if (startPos < 0 || !rdfPred.getValue().equals("<http://xmlns.com/foaf/0.1/knows>"))
				// invalid record or predicate is not a foaf-knows predicate, exit
				return false;
			// read RDF object
			startPos = parseVarLengthEncapsulatedStringField(bytes, startPos, limit, ' ', rdfObj, '"');
			if (startPos < 0)
				// invalid record, exit
				return false;

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

			return true;	
		}
		
		/*
		 * Utility method to efficiently parse encapsulated, variable length strings 
		 */
		private int parseVarLengthEncapsulatedStringField(byte[] bytes, int startPos, int limit, char delim, PactString field, char encaps) {
			
			boolean isEncaps = false;
			
			// check whether string is encapsulated
			if(bytes[startPos] == encaps) {
				isEncaps = true;
			}
			
			if(isEncaps) {
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
	 *  
	 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
	 *
	 */
	@ConstantFields(fields={0})
	public static class BuildTriads extends ReduceStub {
		
		// list of non-matching vertices
		private final ArrayList<PactString> otherVertices = new ArrayList<PactString>(32);
		// matching vertex
		private final PactString matchVertex = new PactString();
		// mutable output record
		private final PactRecord result = new PactRecord();
		
		// initialize list of non-matching vertices for one vertex
		public BuildTriads() {
			this.otherVertices.add(new PactString());
		}

		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception
		{
			// read the first edge
			final PactRecord rec = records.next();
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
				final PactRecord next = records.next();
				
				final PactString myVertex;
				// obtain an object to store the non-matching vertex
				if (numEdges >= this.otherVertices.size()) {
					// we need an additional vertex object
					// create the object
					myVertex = new PactString();
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
					final PactString otherVertex = this.otherVertices.get(i);
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
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
	 */
	@ConstantFieldsFirstExcept(fields={})
	public static class CloseTriads extends MatchStub {

		@Override
		public void match(PactRecord triad, PactRecord missingEdge, Collector<PactRecord> out) throws Exception {
			
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
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String edgeInput = (args.length > 1 ? args[1] : "");
		String output    = (args.length > 2 ? args[2] : "");

		FileDataSource edges = new FileDataSource(EdgeInFormat.class, edgeInput, "BTC Edges");
		
		ReduceContract buildTriads = new ReduceContract.Builder(BuildTriads.class, PactString.class, 0)
			.name("Build Triads")
			.build();

		MatchContract closeTriads = MatchContract.builder(CloseTriads.class, PactString.class, 1, 0)
			.keyField(PactString.class, 2, 1)
			.name("Close Triads")
			.build();
		closeTriads.setParameter("INPUT_LEFT_SHIP_STRATEGY", "SHIP_REPARTITION");
		closeTriads.setParameter("INPUT_RIGHT_SHIP_STRATEGY", "SHIP_REPARTITION");
		closeTriads.setParameter("LOCAL_STRATEGY", "LOCAL_STRATEGY_HASH_BUILD_SECOND");

		FileDataSink triangles = new FileDataSink(RecordOutputFormat.class, output, "Output");
		RecordOutputFormat.configureRecordFormat(triangles)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactString.class, 0)
			.field(PactString.class, 1)
			.field(PactString.class, 2);

		triangles.setInput(closeTriads);
		closeTriads.setSecondInput(edges);
		closeTriads.setFirstInput(buildTriads);
		buildTriads.setInput(edges);

		Plan plan = new Plan(triangles, "Enumerate Triangles");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.PlanAssemblerDescription#getDescription()
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [inputRDFTriples] [outputTriangles]";
	}

}
