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
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.OutputContract.UniqueKey;
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
import eu.stratosphere.pact.common.type.base.PactList;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.compiler.PactCompiler;

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
 *
 */
public class EnumTriangles implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * Simple extension of PactPair to hold an edge defined by the connected nodes represented as PactStrings.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
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
	 * Simple extension of PactList to hold multiple edges.
	 * If the list holds one edge it is just an edge.
	 * If it holds two edges it is a triad (an one-side open triangle).
	 * If it holds three edges it is a closed triangle.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 */
	public static class EdgeList extends PactList<Edge> {

		public String toString() {
			Iterator<Edge> it = this.iterator();
			StringBuilder sb = new StringBuilder("[ ");
			while (it.hasNext()) {
				sb.append("( " + it.next().toString() + " ), ");
			}
			sb.append(" ]");
			return sb.toString();
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
	 */
	public static class EdgeListInFormat extends TextInputFormat<Edge, PactNull> {

		private static final Log LOG = LogFactory.getLog(EdgeListInFormat.class);

		@Override
		public boolean readLine(KeyValuePair<Edge, PactNull> pair, byte[] line) {

			String lineStr = new String(line);
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

			EdgeList edgeList = new EdgeList();
			edgeList.add(edge);

			pair.setKey(edge);
			pair.setValue(new PactNull());

			LOG.debug("Read in: " + pair.getKey() + " :: " + pair.getValue());
			return true;
		}
	}

	/**
	 * Used to write an EdgeList to text file.
	 * All edges of the list are concatenated and serialized to byte string.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 */
	public static class EdgeListOutFormat extends TextOutputFormat<PactNull, EdgeList> {
		private static final Log LOG = LogFactory.getLog(EdgeListOutFormat.class);

		@Override
		public byte[] writeLine(KeyValuePair<PactNull, EdgeList> pair) {
			StringBuilder line = new StringBuilder();

			Iterator<Edge> valueIt = pair.getValue().iterator();
			while (valueIt.hasNext()) {
				PactPair<PactString, PactString> edge = valueIt.next();
				line.append(edge.getFirst().toString() + " " + edge.getSecond().toString());
				if (valueIt.hasNext()) {
					line.append(" ");
				}
			}
			line.append('\n');

			LOG.debug("Writing out: " + pair.getKey() + " :: " + pair.getValue());

			return line.toString().getBytes();
		}
	}

	/**
	 * Transforms key-value pairs of the form (Edge,Null) to (PactString,Edge) where the key becomes the
	 * first node of the input key and the value becomes the input key.
	 * Due to the input format, the first node of the input edge is the lexicographically smaller of both nodes.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 */
	public static class AssignKeys extends MapStub<Edge, PactNull, PactString, Edge> {

		private static final Log LOG = LogFactory.getLog(AssignKeys.class);

		@Override
		public void map(Edge edge, PactNull empty, Collector<PactString, Edge> out) {
			LOG.debug("Emit: " + edge.getFirst() + " :: " + edge);
			out.collect(edge.getFirst(), edge);
		}

	}

	/**
	 * Groups all edges whose smaller node is identical.
	 * All edges are collected and pair-wise combined to triads (open triangles).
	 * A triad is represented as an EdgeList with two elements.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 */
	public static class BuildTriads extends ReduceStub<PactString, Edge, Edge, EdgeList> {

		private static final Log LOG = LogFactory.getLog(BuildTriads.class);

		@Override
		public void reduce(PactString node, Iterator<Edge> edges, Collector<Edge, EdgeList> out) {

			// collect all edges
			LinkedList<Edge> edgesList = new LinkedList<Edge>();
			while (edges.hasNext()) {
				edgesList.add(edges.next());
				LOG.debug("Read: " + node + " :: " + edgesList.getLast());
			}

			// we need at least 2 edges to build a triad
			if (edgesList.size() <= 1) {
				return;
			}

			// enumerate all binary edge combinations, build triads and identify missing edges
			for (int i = 0; i < edgesList.size(); i++) {
				for (int j = i + 1; j < edgesList.size(); j++) {

					// identify nodes for missing edge
					PactString e_i = edgesList.get(i).getSecond();
					PactString e_j = edgesList.get(j).getSecond();

					Edge missingEdge;
					EdgeList triad = new EdgeList();

					// build missing edges. Smaller node goes first, greater second.
					if (e_i.compareTo(e_j) <= 0) {
						missingEdge = new Edge(e_i, e_j);
						triad.add(edgesList.get(i));
						triad.add(edgesList.get(j));
					} else {
						missingEdge = new Edge(e_j, e_i);
						triad.add(edgesList.get(j));
						triad.add(edgesList.get(i));
					}

					LOG.debug("Emit: " + missingEdge + " :: " + triad);

					// emit missing edge and triad
					out.collect(missingEdge, triad);
				}
			}
		}
	}

	/**
	 * Matches all missing edges with existing edges from input.
	 * If the missing edge for a triad is found, the triad is transformed to a triangle by adding the missing edge.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 */
	public static class CloseTriads extends MatchStub<Edge, EdgeList, PactNull, PactNull, EdgeList> {

		private static final Log LOG = LogFactory.getLog(CloseTriads.class);

		@Override
		public void match(Edge missingEdge, EdgeList triad, PactNull empty, Collector<PactNull, EdgeList> out) {
			// close triad with missing edge
			triad.add(missingEdge);
			
			LOG.debug("Emit: " + missingEdge + " :: " + triad);
			
			// emit triangle
			out.collect(new PactNull(), triad);
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

		DataSourceContract<Edge, PactNull> edges = new DataSourceContract<Edge, PactNull>(EdgeListInFormat.class,
			edgeInput);
		edges.setFormatParameter("delimiter", "\n");
		edges.setDegreeOfParallelism(noSubTasks);
		edges.setOutputContract(UniqueKey.class);

		MapContract<Edge, PactNull, PactString, Edge> assignKeys = new MapContract<Edge, PactNull, PactString, Edge>(
			AssignKeys.class, "Assign Keys");
		assignKeys.setDegreeOfParallelism(noSubTasks);

		ReduceContract<PactString, Edge, Edge, EdgeList> buildTriads = new ReduceContract<PactString, Edge, Edge, EdgeList>(
			BuildTriads.class, "Build Triads");
		buildTriads.setDegreeOfParallelism(noSubTasks);

		MatchContract<Edge, EdgeList, PactNull, PactNull, EdgeList> closeTriads = new MatchContract<Edge, EdgeList, PactNull, PactNull, EdgeList>(
			CloseTriads.class, "Close Triads");
		closeTriads.setDegreeOfParallelism(noSubTasks);
		// TODO: remove enforced sort-merge strategy
		closeTriads.getStubParameters().setString(PactCompiler.HINT_LOCAL_STRATEGY,
			PactCompiler.HINT_LOCAL_STRATEGY_SORT);

		DataSinkContract<PactNull, EdgeList> triangles = new DataSinkContract<PactNull, EdgeList>(
			EdgeListOutFormat.class, output);
		triangles.setDegreeOfParallelism(noSubTasks);

		triangles.setInput(closeTriads);
		closeTriads.setSecondInput(edges);
		closeTriads.setFirstInput(buildTriads);
		buildTriads.setInput(assignKeys);
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
