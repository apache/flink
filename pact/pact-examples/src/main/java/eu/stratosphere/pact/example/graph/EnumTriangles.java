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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
	 * The EdgeListInFormat filters all rdf triples with foaf:knows predicates. The subjects and objects URL are
	 * compared.
	 * The lexicographically smaller URL becomes the first part (node) of the edge, the greater one the second part
	 * (node).
	 * Finally, the format emits the edge as key. The value is not required, so its set to NULL.
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
			
			startPos = parseVarLengthEncapsulatedStringField(bytes, startPos, limit, ' ', rdfSubj, '"');
			if (startPos < 0) 
				return false;
			startPos = parseVarLengthEncapsulatedStringField(bytes, startPos, limit, ' ', rdfPred, '"');
			if (startPos < 0 || !rdfPred.getValue().equals("<http://xmlns.com/foaf/0.1/knows>")) 
				return false;
			startPos = parseVarLengthEncapsulatedStringField(bytes, startPos, limit, ' ', rdfObj, '"');
			if (startPos < 0) 
				return false;

			if (rdfSubj.compareTo(rdfObj) <= 0) {
				target.setField(0, rdfSubj);
				target.setField(1, rdfObj);
			} else {
				target.setField(0, rdfObj);
				target.setField(1, rdfSubj);
			}
			
			System.out.println(target.getField(0, PactString.class).getValue() + " - " + target.getField(1, PactString.class).getValue());

			return true;	
		}
		
		private int parseVarLengthEncapsulatedStringField(byte[] bytes, int startPos, int limit, char delim, PactString field, char encaps) {
			
			boolean isEncaps = false;
			
			if(bytes[startPos] == encaps) {
				isEncaps = true;
			}
			
			if(isEncaps) {
				// encaps string
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
				// non-encaps string
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

//	/**
//	 * Builds triads (open triangles) from two edges that share the same key.
//	 * A triad is represented as an EdgeList with two elements.
//	 * 
//	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
//	 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
//	 */
//	public static class BuildTriads extends MatchStub {
//
//		private final PactString joinVertex = new PactString();
//		private final PactString endVertex1 = new PactString();
//		private final PactString endVertex2 = new PactString();
//		
//		@Override
//		public void match(PactRecord value1, PactRecord value2, Collector<PactRecord> out) throws Exception {
//			
//			value1.getFieldInto(0, joinVertex);
//			value1.getFieldInto(1, endVertex1);
//			value2.getFieldInto(1, endVertex2);
//			
//			if (endVertex1.compareTo(endVertex2) <= 0) {
//				value1.setField(2, endVertex2);
//				out.collect(value1);
//			} else {
//				value2.setField(2, endVertex1);
//				out.collect(value2);
//			}
//			
//		}		
//	}
	
	public static class BuildTriads extends ReduceStub
	{
		private final ArrayList<PactString> strings = new ArrayList<PactString>(32);
		private final PactRecord result = new PactRecord();
		private final PactString joinVertex = new PactString();
		
		public BuildTriads() {
			this.strings.add(new PactString());
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.ReduceStub#reduce(java.util.Iterator, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception
		{
			// get the first record and the key
			final PactRecord rec = records.next();
			final PactString key = rec.getField(0, this.joinVertex);
			rec.getFieldInto(1, this.strings.get(0));
			
			this.result.setField(0, key);
			
			int numStrings = 1;
			while (records.hasNext()) {
				final PactRecord next = records.next();
				
				final PactString target;
				if (numStrings >= this.strings.size()) {
					target = new PactString();
					this.strings.add(target);
				} else {
					target = this.strings.get(numStrings);
				}
				
				next.getFieldInto(1, target);
				
				for (int i = 0; i < numStrings; i++) {
					final PactString other = this.strings.get(i);
					if (other.compareTo(target) < 0) {
						this.result.setField(1, other);
						this.result.setField(2, target);
						out.collect(this.result);
					} else {
						next.setField(2, other);
						out.collect(next);
					}
				}
				
				numStrings++;
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
	public static class CloseTriads extends MatchStub {

		private static final Log LOG = LogFactory.getLog(CloseTriads.class);

		@Override
		public void match(PactRecord triad, PactRecord missingEdge, Collector<PactRecord> out) throws Exception {
			
			LOG.debug("Emit: " + missingEdge);
			
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
		
//		MatchContract buildTriads = new MatchContract(BuildTriads.class, PactString.class, 0, 0, "Build Triads");
//		buildTriads.getParameters().setString("selfMatch.crossMode", "TRIANGLE_CROSS_EXCL_DIAG");
		ReduceContract buildTriads = new ReduceContract(BuildTriads.class, PactString.class, 0, "Build Triads");
		
		@SuppressWarnings("unchecked")
		MatchContract closeTriads = new MatchContract(CloseTriads.class, new Class[] {PactString.class, PactString.class}, new int[] {1, 2}, new int[] {0, 1}, "Close Triads");
		closeTriads.setParameter("INPUT_LEFT_SHIP_STRATEGY", "SHIP_REPARTITION");
		closeTriads.setParameter("INPUT_RIGHT_SHIP_STRATEGY", "SHIP_REPARTITION");
		closeTriads.setParameter("LOCAL_STRATEGY", "LOCAL_STRATEGY_HASH_BUILD_SECOND");

		FileDataSink triangles = new FileDataSink(RecordOutputFormat.class, output, "Output");
		triangles.getParameters().setString(RecordOutputFormat.RECORD_DELIMITER_PARAMETER, "\n");
		triangles.getParameters().setString(RecordOutputFormat.FIELD_DELIMITER_PARAMETER, " ");
		triangles.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 3);
		triangles.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
		triangles.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactString.class);
		triangles.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 2, PactString.class);

		triangles.setInput(closeTriads);
		closeTriads.setSecondInput(edges);
		closeTriads.setFirstInput(buildTriads);
		buildTriads.setInput(edges);
//		buildTriads.setFirstInput(edges);
//		buildTriads.setSecondInput(edges);

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
