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

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactPair;
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
	 * Simple extension of PactPair to hold an edge defined by the connected nodes represented as PactStrings.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
	 */
	public static class Edge extends PactPair<PactString, PactString> {

		public Edge() {
			super();
		}

		public Edge(PactString s1, PactString s2) {
			super(s1, s2);
		}

		public String toString() {
			return getFirst().toString() + " " + getSecond();
		}
	}

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
	public static class EdgeListInFormat extends DelimitedInputFormat {

		private static final Log LOG = LogFactory.getLog(EdgeListInFormat.class);

		@Override
		public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {

			String lineStr = new String(bytes, 0, numBytes);
			// replace reduce whitespaces and trim
			lineStr = lineStr.replaceAll("\\s+", " ").trim();
			// build whitespace tokenizer
			StringTokenizer st = new StringTokenizer(lineStr, " ");

			// line must have at least three elements
			if (st.countTokens() < 3)
				return false;

			String rdfSubj = st.nextToken();
			String rdfPred = st.nextToken();
			String rdfObj = st.nextToken();

			// we only want foaf:knows predicates
			if (!rdfPred.equals("<http://xmlns.com/foaf/0.1/knows>"))
				return false;

			Edge edge;

			if (rdfSubj.compareTo(rdfObj) <= 0) {
				edge = new Edge(new PactString(rdfSubj), new PactString(rdfObj));
			} else {
				edge = new Edge(new PactString(rdfObj), new PactString(rdfSubj));
			}

			target.setField(0, edge);

			LOG.debug("Read in: " + edge);
			return true;
		}
	}

	/**
	 * Used to write an EdgeList to text file.
	 * All edges of the list are concatenated and serialized to byte string.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
	 */
	public static class EdgeListOutFormat extends FileOutputFormat {
		private static final Log LOG = LogFactory.getLog(EdgeListOutFormat.class);

		@Override
		public void writeRecord(PactRecord record) throws IOException {
			StringBuilder line = new StringBuilder();

			for (int i = 0; i < 3; i++) {
				Edge edge = record.getField(i, Edge.class);
				line.append(edge.getFirst().toString());
				line.append(" ");
				line.append(edge.getSecond().toString());
				//As long as there is a next record
				if(i < 2) {
					line.append(" ");
				}
			}
			line.append("\n");
			
			stream.write(line.toString().getBytes());
			LOG.debug("Writing out: " + line);
		}
	}

	/**
	 * Transforms key-value pairs of the form (Edge,Null) to (PactString,Edge) where the key becomes the
	 * first node of the input key and the value becomes the input key.
	 * Due to the input format, the first node of the input edge is the lexicographically smaller of both nodes.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
	 */
	public static class AssignKeys extends MapStub {

		private static final Log LOG = LogFactory.getLog(AssignKeys.class);

		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			Edge edge = record.getField(0, Edge.class);
			LOG.debug("Emit: " + edge.getFirst() + " :: " + edge);
			
			record.setField(0, edge.getFirst());
			record.setField(1, edge);
			out.collect(record);
		}
	}

	/**
	 * Builds triads (open triangles) from two edges that share the same key.
	 * A triad is represented as an EdgeList with two elements.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
	 */
	public static class BuildTriads extends MatchStub {

		private static final Log LOG = LogFactory.getLog(BuildTriads.class);

		@Override
		public void match(PactRecord value1, PactRecord value2, Collector out)
				throws Exception {
			Edge edge1 = value1.getField(1, Edge.class);
			Edge edge2 = value2.getField(1, Edge.class);
			
			// we do not connect a node with itself
			if(edge1.compareTo(edge2) <= 0) {
				return;
			}
			
			// identify nodes for missing edge
			PactString missing1 = edge1.getSecond();
			PactString missing2 = edge2.getSecond();

			Edge missingEdge;
			PactRecord outRecord;

			// build missing edges. Smaller node goes first, greater second.
			if (missing1.compareTo(missing2) <= 0) {
				missingEdge = new Edge(missing1, missing2);
				outRecord = value1;
				outRecord.setField(2, edge2);
			} else {
				missingEdge = new Edge(missing2, missing1);
				outRecord = value2;
				outRecord.setField(2, edge1);
			}
			outRecord.setField(0, missingEdge);
			LOG.debug("Emit: " + missingEdge);

			// emit missing edge and triad
			out.collect(outRecord);
			
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
		public void match(PactRecord triad, PactRecord missingEdge, Collector out) throws Exception {
			
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

		FileDataSource edges = new FileDataSource(EdgeListInFormat.class, edgeInput, "RDF Triples");
		edges.setDegreeOfParallelism(noSubTasks);
		//edges.setOutputContract(UniqueKey.class);

		MapContract assignKeys = new MapContract(AssignKeys.class, "Assign Keys");
		assignKeys.setDegreeOfParallelism(noSubTasks);

		MatchContract buildTriads = new MatchContract(BuildTriads.class, PactString.class, 0, 0, "Build Triads");
		buildTriads.setDegreeOfParallelism(noSubTasks);

		MatchContract closeTriads = new MatchContract(CloseTriads.class, Edge.class, 0, 0, "Close Triads");
		closeTriads.setDegreeOfParallelism(noSubTasks);

		FileDataSink triangles = new FileDataSink(EdgeListOutFormat.class, output, "Triangles");
		triangles.setDegreeOfParallelism(noSubTasks);

		triangles.setInput(closeTriads);
		closeTriads.setSecondInput(edges);
		closeTriads.setFirstInput(buildTriads);
		buildTriads.setFirstInput(assignKeys);
		buildTriads.setSecondInput(assignKeys);
		assignKeys.setInput(edges);

		return new Plan(triangles, "Enumerate Triangles");

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
