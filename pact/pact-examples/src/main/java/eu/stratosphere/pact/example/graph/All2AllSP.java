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
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.OutputContract.UniqueKey;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class All2AllSP implements PlanAssembler, PlanAssemblerDescription {

	public static class Edge implements Key, Value {

		private int vertex1;

		private int vertex2;

		public Edge(int vertex1, int vertex2) {
			this.vertex1 = vertex1;
			this.vertex2 = vertex2;
		}

		public Edge() {
		}

		public int getVertex1() {
			return vertex1;
		}

		public int getVertex2() {
			return vertex2;
		}

		@Override
		public void read(DataInput in) throws IOException {
			vertex1 = in.readInt();
			vertex2 = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(vertex1);
			out.writeInt(vertex2);
		}

		@Override
		public int compareTo(Key o) {
			if (!(o instanceof Edge)) {
				return -1;
			}
			Edge compEdge = (Edge) o;
			int compVal = vertex1 - compEdge.vertex1;
			if (compVal == 0) {
				compVal = vertex2 - compEdge.vertex2;
			}
			return compVal;
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof Edge) {
				Edge r = (Edge) o;
				return (vertex1 == r.vertex1 && vertex2 == r.vertex2);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			long h = ((vertex1 & 0xffffffffL) << 32) | (vertex2 & 0xffffffffL) << 0;
			return (int) (h ^ (h >>> 32));
		}

		@Override
		public String toString() {
			return "<" + (vertex1) + "|" + (vertex2) + ">";
		}
	}

	public static class ConnectionInfo implements Value {

		int fromVertex;

		int toVertex;

		int distance;

		int[] hopsList;

		int hopCnt;

		public ConnectionInfo() {
			hopsList = new int[8];
			hopCnt = 0;
		}

		public ConnectionInfo(int fromVertex, int toVertex, int distance, int[] hopsList, int hopCnt) {
			this.fromVertex = fromVertex;
			this.toVertex = toVertex;
			this.distance = distance;
			this.hopsList = hopsList;
			this.hopCnt = hopCnt;
		}

		public int getFromVertex() {
			return fromVertex;
		}

		public int getToVertex() {
			return toVertex;
		}

		public int getDistance() {
			return distance;
		}

		public int[] getHopsList() {
			return hopsList;
		}

		public int getHopCnt() {
			return hopCnt;
		}

		public void setFromVertex(int fromVertex) {
			this.fromVertex = fromVertex;
		}

		public void setToVertex(int toVertex) {
			this.toVertex = toVertex;
		}

		public void setDistance(int distance) {
			this.distance = distance;
		}

		public void addHop(int hop) {
			if (hopCnt == hopsList.length) {
				int[] newHopList = new int[2 * hopCnt];
				for (int i = 0; i < hopCnt; i++) {
					newHopList[i] = hopsList[i];
				}
				hopsList = newHopList;
			}
			this.hopsList[hopCnt++] = hop;
		}

		@Override
		public void read(DataInput in) throws IOException {
			fromVertex = in.readInt();
			toVertex = in.readInt();
			distance = in.readInt();
			hopCnt = in.readInt();
			if (hopCnt < 8) {
				hopsList = new int[8];
			} else {
				hopsList = new int[hopCnt * 2];
			}
			for (int i = 0; i < hopCnt; i++) {
				hopsList[i] = in.readInt();
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(fromVertex);
			out.writeInt(toVertex);
			out.writeInt(distance);
			out.writeInt(hopCnt);
			for (int i = 0; i < hopCnt; i++) {
				out.writeInt(hopsList[i]);
			}
		}

		@Override
		public String toString() {
			StringBuilder returnString = new StringBuilder(fromVertex + "|" + toVertex + "|" + distance + "|");
			for (int i = 0; i < hopCnt; i++) {
				returnString.append(hopsList[i]);
				if (i + 1 < hopCnt) {
					returnString.append(' ');
				}
			}
			returnString.append('|');

			return returnString.toString();
		}
	}

	/**
	 * Reads and writes connection data in the following format:
	 * <string:fromVertice>|<string:toVertice>|<int:distance>|<string:hops>|
	 * The connection info is completeley hold in the value.
	 * The key holds the pair of fromVertice and toVertice.
	 * 
	 * @author fhueske
	 */
	public static class PathInFormat extends TextInputFormat<Edge, ConnectionInfo> {

		private static final Log LOG = LogFactory.getLog(PathInFormat.class);

		@Override
		public boolean readLine(KeyValuePair<Edge, ConnectionInfo> pair, byte[] line) {

			int start = 0;
			int valueCnt = 0;

			String fromVertex = null;
			String toVertex = null;
			int distance = 0;
			String hopsList = null;

			ConnectionInfo conn = new ConnectionInfo();
			for (int pos = 0; pos < line.length; pos++) {
				if (line[pos] == '|') {
					if (valueCnt == 0) {
						// fromVertex
						fromVertex = new String(line, start, pos - start);
						conn.setFromVertex(fromVertex.hashCode());
					} else if (valueCnt == 1) {
						// toVertex
						toVertex = new String(line, start, pos - start);
						conn.setToVertex(toVertex.hashCode());
					} else if (valueCnt == 2) {
						// distance
						distance = Integer.parseInt(new String(line, start, pos - start));
						conn.setDistance(distance);
					} else {
						// hopList
						hopsList = new String(line, start, pos - start);
						StringTokenizer st = new StringTokenizer(hopsList, " ");
						while (st.hasMoreTokens()) {
							conn.addHop(Integer.parseInt(st.nextToken()));
						}
					}
					start = pos + 1;
					valueCnt++;
				}
			}

			pair.setKey(new Edge(fromVertex.hashCode(), toVertex.hashCode()));
			pair.setValue(conn);

			LOG.info("Read in: [" + pair.getKey() + "," + pair.getValue() + "]");
			return true;
		}
	}

	public static class PathOutFormat extends TextOutputFormat<Edge, ConnectionInfo> {

		private static final Log LOG = LogFactory.getLog(PathInFormat.class);

		@Override
		public byte[] writeLine(KeyValuePair<Edge, ConnectionInfo> pair) {
			StringBuilder line = new StringBuilder();
			line.append(pair.getValue().toString() + "\n");

			LOG.info("Writing out: [" + pair.getKey() + "," + pair.getValue() + "]");

			return line.toString().getBytes();
		}

	}

	public static class ProjectPathStart extends MapStub<Edge, ConnectionInfo, PactInteger, ConnectionInfo> {

		private static final Log LOG = LogFactory.getLog(ProjectPathStart.class);

		@Override
		protected void map(Edge key, ConnectionInfo value, Collector<PactInteger, ConnectionInfo> out) {
			LOG.info("Processing: [" + key + "," + value + "]");
			out.collect(new PactInteger(key.getVertex1()), value);
			LOG.info("Emitted: [" + key.getVertex1() + "," + value + "]");
		}
	}

	public static class ProjectPathEnd extends MapStub<Edge, ConnectionInfo, PactInteger, ConnectionInfo> {

		private static final Log LOG = LogFactory.getLog(ProjectPathEnd.class);

		@Override
		protected void map(Edge key, ConnectionInfo value, Collector<PactInteger, ConnectionInfo> out) {
			LOG.info("Processing: [" + key + "," + value + "]");
			out.collect(new PactInteger(key.getVertex2()), value);
			LOG.info("Emitted: [" + key.getVertex2() + "," + value + "]");
		}
	}

	public static class ConcatPath extends MatchStub<PactInteger, ConnectionInfo, ConnectionInfo, Edge, ConnectionInfo> {

		private static final Log LOG = LogFactory.getLog(ConcatPath.class);

		@Override
		public void match(PactInteger key, ConnectionInfo value1, ConnectionInfo value2,
				Collector<Edge, ConnectionInfo> out) {

			LOG.info("Processing: [" + key + "," + value1 + "] + [" + key + "," + value2 + "]");

			int fromVertex = value2.getFromVertex();
			int toVertex = value1.getToVertex();
			if (fromVertex == toVertex)
				return;

			ConnectionInfo value = new ConnectionInfo();
			value.setFromVertex(fromVertex);
			value.setToVertex(toVertex);
			value.setDistance(value1.getDistance() + value2.getDistance());

			for (int i = 0; i < value2.getHopCnt(); i++) {
				value.addHop(value2.getHopsList()[i]);
			}
			value.addHop(key.getValue());
			for (int i = 0; i < value1.getHopCnt(); i++) {
				value.addHop(value1.getHopsList()[i]);
			}

			out.collect(new Edge(fromVertex, toVertex), value);
			LOG.info("Emitted: [<" + fromVertex + "|" + toVertex + ">," + value + "]");
		}
	}

	@SameKey
	public static class FindShortestPath extends
			CoGroupStub<Edge, ConnectionInfo, ConnectionInfo, Edge, ConnectionInfo> {

		private static final Log LOG = LogFactory.getLog(FindShortestPath.class);

		@Override
		public void coGroup(Edge key, Iterator<ConnectionInfo> values1, Iterator<ConnectionInfo> values2,
				Collector<Edge, ConnectionInfo> out) {

			int minDist = Integer.MAX_VALUE;
			ConnectionInfo minDistValue = null;

			while (values1.hasNext()) {
				ConnectionInfo value = values1.next();
				LOG.info("Processing: [" + key + "," + value + "]");
				int dist = value.getDistance();
				if (dist < minDist) {
					minDist = dist;
					minDistValue = value;
				}
			}

			while (values2.hasNext()) {
				ConnectionInfo value = values2.next();
				LOG.info("Processing: [" + key + "," + value + "]");
				int dist = value.getDistance();
				if (dist < minDist) {
					minDist = dist;
					minDistValue = value;
				}
			}

			out.collect(key, minDistValue);
			LOG.info("Emitted: [" + key + "," + minDistValue + "]");
		}
	}

	@Override
	public Plan getPlan(String... args) {

		// check for the correct number of job parameters
		if (args.length != 3) {
			return null;
		}

		int noSubTasks = Integer.parseInt(args[0]);
		String paths = args[1];
		String output = args[2];

		DataSourceContract<Edge, ConnectionInfo> pathsInput = new DataSourceContract<Edge, ConnectionInfo>(
			PathInFormat.class, paths);
		pathsInput.setFormatParameter("delimiter", "\n");
		pathsInput.setDegreeOfParallelism(noSubTasks);
		pathsInput.setOutputContract(UniqueKey.class);

		MapContract<Edge, ConnectionInfo, PactInteger, ConnectionInfo> pathStarts = new MapContract<Edge, ConnectionInfo, PactInteger, ConnectionInfo>(
			ProjectPathStart.class, "Project Starts");
		pathStarts.setDegreeOfParallelism(noSubTasks);

		MapContract<Edge, ConnectionInfo, PactInteger, ConnectionInfo> pathEnds = new MapContract<Edge, ConnectionInfo, PactInteger, ConnectionInfo>(
			ProjectPathEnd.class, "Project Ends");
		pathEnds.setDegreeOfParallelism(noSubTasks);

		MatchContract<PactInteger, ConnectionInfo, ConnectionInfo, Edge, ConnectionInfo> concatPaths = new MatchContract<PactInteger, ConnectionInfo, ConnectionInfo, Edge, ConnectionInfo>(
			ConcatPath.class, "Concat Paths");
		concatPaths.setDegreeOfParallelism(noSubTasks);

		CoGroupContract<Edge, ConnectionInfo, ConnectionInfo, Edge, ConnectionInfo> findShortestPaths = new CoGroupContract<Edge, ConnectionInfo, ConnectionInfo, Edge, ConnectionInfo>(
			FindShortestPath.class, "Find Shortest Paths");
		findShortestPaths.setDegreeOfParallelism(noSubTasks);

		DataSinkContract<Edge, ConnectionInfo> result = new DataSinkContract<Edge, ConnectionInfo>(PathOutFormat.class,
			output);
		result.setDegreeOfParallelism(noSubTasks);

		result.setInput(findShortestPaths);
		findShortestPaths.setFirstInput(pathsInput);
		findShortestPaths.setSecondInput(concatPaths);
		concatPaths.setFirstInput(pathStarts);
		pathStarts.setInput(pathsInput);
		concatPaths.setSecondInput(pathEnds);
		pathEnds.setInput(pathsInput);

		return new Plan(result, "All2All Shortest Paths");

	}

	@Override
	public String getDescription() {
		return "Parameters: dop, input-paths, output-paths";
	}

}
