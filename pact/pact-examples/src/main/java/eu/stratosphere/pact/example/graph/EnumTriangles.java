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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.OutputContract;
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
import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;

public class EnumTriangles implements PlanAssembler, PlanAssemblerDescription {

	public static class N_StringPair extends PactPair<PactString, PactString> {
		public N_StringPair() {
			super();
		}

		public N_StringPair(PactString s1, PactString s2) {
			super(s1, s2);
		}

		public String toString() {
			return "<" + getFirst().toString() + "|" + getSecond() + ">";
		}
	}

	public static class N_List_StringPair extends PactList<N_StringPair> {
		public String toString() {
			Iterator<N_StringPair> it = this.iterator();
			StringBuilder sb = new StringBuilder("[");
			while (it.hasNext()) {
				sb.append(it.next().toString() + " , ");
			}
			sb.append("]");
			return sb.toString();
		}
	}

	/**
	 * Reads and writes lists of edges data in the following format:
	 * <string:fromVertice1>|<string:toVertice1>|<string:fromVertice2>|<string:toVertice2>|...|
	 * The list is completeley hold in the value. Each edge is represented as a pair of strings.
	 * Within the pair the lexiographic smaller vertice is the first pair element.
	 * The key holds the pair vertices of the first edge.
	 * 
	 * @author fhueske
	 */
	public static class EdgeListInFormat extends TextInputFormat<N_StringPair, N_List_StringPair> {

		private static final Log LOG = LogFactory.getLog(EdgeListInFormat.class);

		@Override
		public boolean readLine(KeyValuePair<N_StringPair, N_List_StringPair> pair, byte[] line) {

			int start = 0;

			PactString firstVertice = null;
			N_List_StringPair values = new N_List_StringPair();
			for (int pos = 0; pos < line.length; pos++) {
				if (line[pos] == '|') {
					if (firstVertice == null) {
						// first vertice
						firstVertice = new PactString(new String(line, start, pos - start));
					} else {
						// second vertice -> add pair
						PactString secondVertice = new PactString(new String(line, start, pos - start));
						// decide which vertice is first element of pair
						N_StringPair edge;
						if (firstVertice.compareTo(secondVertice) <= 0) {
							edge = new N_StringPair(firstVertice, secondVertice);
						} else {
							edge = new N_StringPair(secondVertice, firstVertice);
						}
						values.add(edge);
						// reset value counter
						firstVertice = null;
					}
					start = pos + 1;
				}
			}
			pair.setKey(values.get(0));
			pair.setValue(values);

			LOG.info("Read in: [" + pair.getKey() + "," + pair.getValue() + "]");
			return true;
		}
	}

	public static class EdgeListOutFormat extends TextOutputFormat<N_StringPair, N_List_StringPair> {
		private static final Log LOG = LogFactory.getLog(EdgeListOutFormat.class);

		@Override
		public byte[] writeLine(KeyValuePair<N_StringPair, N_List_StringPair> pair) {
			StringBuilder line = new StringBuilder();

			Iterator<N_StringPair> valueIt = pair.getValue().iterator();
			while (valueIt.hasNext()) {
				PactPair<PactString, PactString> edge = valueIt.next();
				line.append(edge.getFirst().toString() + "|" + edge.getSecond().toString() + "|");
			}
			line.append('\n');

			LOG.info("Writing out: [" + pair.getKey() + "," + pair.getValue() + "]");

			return line.toString().getBytes();
		}

	}

	public static class AssignKeys extends MapStub<N_StringPair, N_List_StringPair, PactString, N_List_StringPair> {

		private static final Log LOG = LogFactory.getLog(AssignKeys.class);

		@Override
		public void map(N_StringPair edge, N_List_StringPair edgeList, Collector<PactString, N_List_StringPair> out) {
			out.collect(edge.getFirst(), edgeList);
			LOG.info("Processed: [" + edge.getFirst() + "," + edgeList + "]");
		}

	}

	public static class BuildTriads extends ReduceStub<PactString, N_List_StringPair, N_StringPair, N_List_StringPair> {

		private static final Log LOG = LogFactory.getLog(BuildTriads.class);

		@Override
		public void reduce(PactString vertice, Iterator<N_List_StringPair> edgeListsList,
				Collector<N_StringPair, N_List_StringPair> out) {
			// collect all edges
			LinkedList<N_StringPair> edges = new LinkedList<N_StringPair>();
			while (edgeListsList.hasNext()) {
				// each edge list contains only one edge
				N_StringPair value = edgeListsList.next().get(0);
				edges.add(value);

				LOG.info("Processed: [" + vertice + "," + value + "]");
			}

			// enumerate all binary edge combinations and build triangles
			for (int i = 0; i < edges.size(); i++) {
				for (int j = i + 1; j < edges.size(); j++) {
					// find missing edge
					PactString v_i = edges.get(i).getSecond();
					PactString v_j = edges.get(j).getSecond();

					N_StringPair missingEdge;
					N_List_StringPair triad = new N_List_StringPair();

					if (v_i.compareTo(v_j) <= 0) {
						missingEdge = new N_StringPair(v_i, v_j);
						triad.add(edges.get(i));
						triad.add(edges.get(j));
					} else {
						missingEdge = new N_StringPair(v_j, v_i);
						triad.add(edges.get(j));
						triad.add(edges.get(i));
					}

					LOG.info("Emitted: [" + missingEdge + "," + triad + "]");

					out.collect(missingEdge, triad);
				}
			}
		}
	}

	@OutputContract.SameKey
	public static class CloseTriads extends
			MatchStub<N_StringPair, N_List_StringPair, N_List_StringPair, N_StringPair, N_List_StringPair> {

		private static final Log LOG = LogFactory.getLog(CloseTriads.class);

		@Override
		public void match(N_StringPair missingEdge, N_List_StringPair triad, N_List_StringPair mev,
				Collector<N_StringPair, N_List_StringPair> out) {
			// close triad with missing edge
			triad.add(missingEdge);
			// emit triangle
			out.collect(missingEdge, triad);

			LOG.info("Processed: [" + missingEdge + "," + triad + "] + [" + missingEdge + "," + mev + "]");
		}

	}

	@Override
	public Plan getPlan(String... args) {

		// check for the correct number of job parameters
		if (args.length != 3) {
			return null;
		}

		int noSubTasks = Integer.parseInt(args[0]);
		String edgeInput = args[1];
		String output = args[2];

		DataSourceContract<N_StringPair, N_List_StringPair> edges = new DataSourceContract<N_StringPair, N_List_StringPair>(
			EdgeListInFormat.class, edgeInput);
		edges.setFormatParameter("delimiter", "\n");
		edges.setDegreeOfParallelism(noSubTasks);
		edges.setOutputContract(UniqueKey.class);

		MapContract<N_StringPair, N_List_StringPair, PactString, N_List_StringPair> assignKeys = new MapContract<N_StringPair, N_List_StringPair, PactString, N_List_StringPair>(
			AssignKeys.class, "Assign Keys");
		assignKeys.setDegreeOfParallelism(noSubTasks);

		ReduceContract<PactString, N_List_StringPair, N_StringPair, N_List_StringPair> buildTriads = new ReduceContract<PactString, N_List_StringPair, N_StringPair, N_List_StringPair>(
			BuildTriads.class, "Build Triads");
		buildTriads.setDegreeOfParallelism(noSubTasks);

		MatchContract<N_StringPair, N_List_StringPair, N_List_StringPair, N_StringPair, N_List_StringPair> closeTriads = new MatchContract<N_StringPair, N_List_StringPair, N_List_StringPair, N_StringPair, N_List_StringPair>(
			CloseTriads.class, "Close Triads");
		closeTriads.setDegreeOfParallelism(noSubTasks);

		DataSinkContract<N_StringPair, N_List_StringPair> triangles = new DataSinkContract<N_StringPair, N_List_StringPair>(
			EdgeListOutFormat.class, output);
		triangles.setDegreeOfParallelism(noSubTasks);

		triangles.setInput(closeTriads);
		closeTriads.setSecondInput(edges);
		closeTriads.setFirstInput(buildTriads);
		buildTriads.setInput(assignKeys);
		assignKeys.setInput(edges);

		return new Plan(triangles, "Enumerate Triangles");

	}

	@Override
	public String getDescription() {
		return "Parameters: dop, input-edges, result-triangles";
	}

}
