/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.test;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

public class TestGraphUtils {

	public static final DataSet<Vertex<Long, Long>> getLongLongVertexData(
			ExecutionEnvironment env) {

		return env.fromCollection(getLongLongVertices());
	}
	
	public static final DataSet<Edge<Long, Long>> getLongLongEdgeData(
			ExecutionEnvironment env) {

		return env.fromCollection(getLongLongEdges());
	}

	public static final DataSet<Edge<Long, Long>> getLongLongEdgeInvalidSrcData(
			ExecutionEnvironment env) {
		List<Edge<Long, Long>> edges = getLongLongEdges();

		edges.remove(1);
		edges.add(new Edge<Long, Long>(13L, 3L, 13L));

		return env.fromCollection(edges);
	}

	public static final DataSet<Edge<Long, Long>> getLongLongEdgeInvalidTrgData(
			ExecutionEnvironment env) {
		List<Edge<Long, Long>> edges =  getLongLongEdges();

		edges.remove(0);
		edges.add(new Edge<Long, Long>(3L, 13L, 13L));

		return env.fromCollection(edges);
	}

	public static final DataSet<Edge<Long, Long>> getLongLongEdgeInvalidSrcTrgData(
			ExecutionEnvironment env) {
		List<Edge<Long, Long>> edges = getLongLongEdges();
		edges.remove(0);
		edges.remove(1);
		edges.remove(2);
		edges.add(new Edge<Long, Long>(13L, 3L, 13L));
		edges.add(new Edge<Long, Long>(1L, 12L, 12L));
		edges.add(new Edge<Long, Long>(13L, 33L, 13L));
		return env.fromCollection(edges);
	}

	public static final DataSet<Edge<String, Long>> getStringLongEdgeData(
			ExecutionEnvironment env) {
		List<Edge<String, Long>> edges = new ArrayList<Edge<String, Long>>();
		edges.add(new Edge<String, Long>("1", "2", 12L));
		edges.add(new Edge<String, Long>("1", "3", 13L));
		edges.add(new Edge<String, Long>("2", "3", 23L));
		edges.add(new Edge<String, Long>("3", "4", 34L));
		edges.add(new Edge<String, Long>("3", "5", 35L));
		edges.add(new Edge<String, Long>("4", "5", 45L));
		edges.add(new Edge<String, Long>("5", "1", 51L));
		return env.fromCollection(edges);
	}

	public static final DataSet<Tuple2<Long, Long>> getLongLongTuple2Data(
			ExecutionEnvironment env) {
		List<Tuple2<Long, Long>> tuples = new ArrayList<Tuple2<Long, Long>>();
		tuples.add(new Tuple2<Long, Long>(1L, 10L));
		tuples.add(new Tuple2<Long, Long>(2L, 20L));
		tuples.add(new Tuple2<Long, Long>(3L, 30L));
		tuples.add(new Tuple2<Long, Long>(4L, 40L));
		tuples.add(new Tuple2<Long, Long>(6L, 60L));

		return env.fromCollection(tuples);
	}

	public static final DataSet<Tuple2<Long, Long>> getLongLongTuple2SourceData(
			ExecutionEnvironment env) {
		List<Tuple2<Long, Long>> tuples = new ArrayList<Tuple2<Long, Long>>();
		tuples.add(new Tuple2<Long, Long>(1L, 10L));
		tuples.add(new Tuple2<Long, Long>(1L, 20L));
		tuples.add(new Tuple2<Long, Long>(2L, 30L));
		tuples.add(new Tuple2<Long, Long>(3L, 40L));
		tuples.add(new Tuple2<Long, Long>(3L, 50L));
		tuples.add(new Tuple2<Long, Long>(4L, 60L));
		tuples.add(new Tuple2<Long, Long>(6L, 70L));

		return env.fromCollection(tuples);
	}

	public static final DataSet<Tuple2<Long, Long>> getLongLongTuple2TargetData(
			ExecutionEnvironment env) {
		List<Tuple2<Long, Long>> tuples = new ArrayList<Tuple2<Long, Long>>();
		tuples.add(new Tuple2<Long, Long>(2L, 10L));
		tuples.add(new Tuple2<Long, Long>(3L, 20L));
		tuples.add(new Tuple2<Long, Long>(3L, 30L));
		tuples.add(new Tuple2<Long, Long>(4L, 40L));
		tuples.add(new Tuple2<Long, Long>(6L, 50L));
		tuples.add(new Tuple2<Long, Long>(6L, 60L));
		tuples.add(new Tuple2<Long, Long>(1L, 70L));

		return env.fromCollection(tuples);
	}

	public static final DataSet<Tuple3<Long, Long, Long>> getLongLongLongTuple3Data(
			ExecutionEnvironment env) {
		List<Tuple3<Long, Long, Long>> tuples = new ArrayList<Tuple3<Long, Long, Long>>();
		tuples.add(new Tuple3<Long, Long, Long>(1L, 2L, 12L));
		tuples.add(new Tuple3<Long, Long, Long>(1L, 3L, 13L));
		tuples.add(new Tuple3<Long, Long, Long>(2L, 3L, 23L));
		tuples.add(new Tuple3<Long, Long, Long>(3L, 4L, 34L));
		tuples.add(new Tuple3<Long, Long, Long>(3L, 6L, 36L));
		tuples.add(new Tuple3<Long, Long, Long>(4L, 6L, 46L));
		tuples.add(new Tuple3<Long, Long, Long>(6L, 1L, 61L));

		return env.fromCollection(tuples);
	}

	public static final DataSet<Tuple2<Long, DummyCustomParameterizedType<Float>>> getLongCustomTuple2Data(
			ExecutionEnvironment env) {
		List<Tuple2<Long, DummyCustomParameterizedType<Float>>> tuples = new ArrayList<Tuple2<Long,
				DummyCustomParameterizedType<Float>>>();
		tuples.add(new Tuple2<Long, DummyCustomParameterizedType<Float>>(1L,
				new DummyCustomParameterizedType<Float>(10, 10f)));
		tuples.add(new Tuple2<Long, DummyCustomParameterizedType<Float>>(2L,
				new DummyCustomParameterizedType<Float>(20, 20f)));
		tuples.add(new Tuple2<Long, DummyCustomParameterizedType<Float>>(3L,
				new DummyCustomParameterizedType<Float>(30, 30f)));
		tuples.add(new Tuple2<Long, DummyCustomParameterizedType<Float>>(4L,
				new DummyCustomParameterizedType<Float>(40, 40f)));
		return env.fromCollection(tuples);
	}

	public static final DataSet<Tuple2<Long, DummyCustomParameterizedType<Float>>> getLongCustomTuple2SourceData(
			ExecutionEnvironment env) {
		List<Tuple2<Long, DummyCustomParameterizedType<Float>>> tuples = new ArrayList<Tuple2<Long,
				DummyCustomParameterizedType<Float>>>();
		tuples.add(new Tuple2<Long, DummyCustomParameterizedType<Float>>(1L,
				new DummyCustomParameterizedType<Float>(10, 10f)));
		tuples.add(new Tuple2<Long, DummyCustomParameterizedType<Float>>(1L,
				new DummyCustomParameterizedType<Float>(20, 20f)));
		tuples.add(new Tuple2<Long, DummyCustomParameterizedType<Float>>(2L,
				new DummyCustomParameterizedType<Float>(30, 30f)));
		tuples.add(new Tuple2<Long, DummyCustomParameterizedType<Float>>(3L,
				new DummyCustomParameterizedType<Float>(40, 40f)));

		return env.fromCollection(tuples);
	}

	public static final DataSet<Tuple2<Long, DummyCustomParameterizedType<Float>>> getLongCustomTuple2TargetData(
			ExecutionEnvironment env) {
		List<Tuple2<Long, DummyCustomParameterizedType<Float>>> tuples = new ArrayList<Tuple2<Long,
				DummyCustomParameterizedType<Float>>>();
		tuples.add(new Tuple2<Long, DummyCustomParameterizedType<Float>>(2L,
				new DummyCustomParameterizedType<Float>(10, 10f)));
		tuples.add(new Tuple2<Long, DummyCustomParameterizedType<Float>>(3L,
				new DummyCustomParameterizedType<Float>(20, 20f)));
		tuples.add(new Tuple2<Long, DummyCustomParameterizedType<Float>>(3L,
				new DummyCustomParameterizedType<Float>(30, 30f)));
		tuples.add(new Tuple2<Long, DummyCustomParameterizedType<Float>>(4L,
				new DummyCustomParameterizedType<Float>(40, 40f)));

		return env.fromCollection(tuples);
	}

	public static final DataSet<Tuple3<Long, Long, DummyCustomParameterizedType<Float>>> getLongLongCustomTuple3Data(
			ExecutionEnvironment env) {
		List<Tuple3<Long, Long, DummyCustomParameterizedType<Float>>> tuples = 
				new ArrayList<Tuple3<Long, Long, DummyCustomParameterizedType<Float>>>();
		tuples.add(new Tuple3<Long, Long, DummyCustomParameterizedType<Float>>(1L, 2L,
				new DummyCustomParameterizedType<Float>(10, 10f)));
		tuples.add(new Tuple3<Long, Long, DummyCustomParameterizedType<Float>>(1L, 3L,
				new DummyCustomParameterizedType<Float>(20, 20f)));
		tuples.add(new Tuple3<Long, Long, DummyCustomParameterizedType<Float>>(2L, 3L,
				new DummyCustomParameterizedType<Float>(30, 30f)));
		tuples.add(new Tuple3<Long, Long, DummyCustomParameterizedType<Float>>(3L, 4L,
				new DummyCustomParameterizedType<Float>(40, 40f)));

		return env.fromCollection(tuples);
	}

	/**
	 * A graph with invalid vertex ids
	 */
	public static final DataSet<Vertex<Long, Long>> getLongLongInvalidVertexData(
			ExecutionEnvironment env) {
		List<Vertex<Long, Long>> vertices = getLongLongVertices();

		vertices.remove(0);
		vertices.add(new Vertex<Long, Long>(15L, 1L));

		return env.fromCollection(vertices);
	}

	/**
	 * A graph that has at least one vertex with no ingoing/outgoing edges
	 */
	public static final DataSet<Edge<Long, Long>> getLongLongEdgeDataWithZeroDegree(
			ExecutionEnvironment env) {
		List<Edge<Long, Long>> edges = new ArrayList<Edge<Long, Long>>();
		edges.add(new Edge<Long, Long>(1L, 2L, 12L));
		edges.add(new Edge<Long, Long>(1L, 4L, 14L));
		edges.add(new Edge<Long, Long>(1L, 5L, 15L));
		edges.add(new Edge<Long, Long>(2L, 3L, 23L));
		edges.add(new Edge<Long, Long>(3L, 5L, 35L));
		edges.add(new Edge<Long, Long>(4L, 5L, 45L));

		return env.fromCollection(edges);
	}

	/**
	 * Function that produces an ArrayList of vertices
	 */
	public static final List<Vertex<Long, Long>> getLongLongVertices() {
		List<Vertex<Long, Long>> vertices = new ArrayList<Vertex<Long, Long>>();
		vertices.add(new Vertex<Long, Long>(1L, 1L));
		vertices.add(new Vertex<Long, Long>(2L, 2L));
		vertices.add(new Vertex<Long, Long>(3L, 3L));
		vertices.add(new Vertex<Long, Long>(4L, 4L));
		vertices.add(new Vertex<Long, Long>(5L, 5L));

		return vertices;
	}

	public static final List<Vertex<Long, Boolean>> getLongBooleanVertices() {
		List<Vertex<Long, Boolean>> vertices = new ArrayList<Vertex<Long, Boolean>>();
		vertices.add(new Vertex<Long, Boolean>(1L, true));
		vertices.add(new Vertex<Long, Boolean>(2L, true));
		vertices.add(new Vertex<Long, Boolean>(3L, true));
		vertices.add(new Vertex<Long, Boolean>(4L, true));
		vertices.add(new Vertex<Long, Boolean>(5L, true));

		return vertices;
	}

	public static final List<Vertex<Long, Tuple3<Long, Long, Boolean>>> getLongVerticesWithDegrees() {
		List<Vertex<Long, Tuple3<Long, Long, Boolean>>> vertices = new ArrayList<Vertex<Long, Tuple3<Long, Long, Boolean>>>();
		vertices.add(new Vertex<Long, Tuple3<Long, Long, Boolean>>(1L, new Tuple3<Long, Long, Boolean>(1L, 2L, true)));
		vertices.add(new Vertex<Long, Tuple3<Long, Long, Boolean>>(2L, new Tuple3<Long, Long, Boolean>(1L, 1L, true)));
		vertices.add(new Vertex<Long, Tuple3<Long, Long, Boolean>>(3L, new Tuple3<Long, Long, Boolean>(2L, 2L, true)));
		vertices.add(new Vertex<Long, Tuple3<Long, Long, Boolean>>(4L, new Tuple3<Long, Long, Boolean>(1L, 1L, true)));
		vertices.add(new Vertex<Long, Tuple3<Long, Long, Boolean>>(5L, new Tuple3<Long, Long, Boolean>(2L, 1L, true)));

		return vertices;
	}

	public static final DataSet<Edge<Long, Long>> getDisconnectedLongLongEdgeData(
				ExecutionEnvironment env) {
			List<Edge<Long, Long>> edges = new ArrayList<Edge<Long, Long>>();
			edges.add(new Edge<Long, Long>(1L, 2L, 12L));
			edges.add(new Edge<Long, Long>(1L, 3L, 13L));
			edges.add(new Edge<Long, Long>(2L, 3L, 23L));
			edges.add(new Edge<Long, Long>(4L, 5L, 45L));
			
			return env.fromCollection(edges);
		}
	
	/**
	 * Function that produces an ArrayList of edges
	 */
	public static final List<Edge<Long, Long>> getLongLongEdges() {
		List<Edge<Long, Long>> edges = new ArrayList<Edge<Long, Long>>();
		edges.add(new Edge<Long, Long>(1L, 2L, 12L));
		edges.add(new Edge<Long, Long>(1L, 3L, 13L));
		edges.add(new Edge<Long, Long>(2L, 3L, 23L));
		edges.add(new Edge<Long, Long>(3L, 4L, 34L));
		edges.add(new Edge<Long, Long>(3L, 5L, 35L));
		edges.add(new Edge<Long, Long>(4L, 5L, 45L));
		edges.add(new Edge<Long, Long>(5L, 1L, 51L));
	
		return edges;
	}

	public static class DummyCustomType implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private int intField;
		private boolean booleanField;
		
		public DummyCustomType(int intF, boolean boolF) {
			this.intField = intF;
			this.booleanField = boolF;
		}
		
		public DummyCustomType() {
			this.intField = 0;
			this.booleanField = true;
		}

		public int getIntField() {
			return intField;
		}
		
		public void setIntField(int intF) {
			this.intField = intF;
		}
		
		public boolean getBooleanField() {
			return booleanField;
		}
		
		@Override
		public String toString() {
			return booleanField ? "(T," + intField + ")" : "(F," + intField + ")";
		}
	}
	
	public static class DummyCustomParameterizedType<T> implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private int intField;
		private T tField;
		
		public DummyCustomParameterizedType(int intF, T tF) {
			this.intField = intF;
			this.tField = tF;
		}
		
		public DummyCustomParameterizedType() {
			this.intField = 0;
			this.tField = null;
		}

		public int getIntField() {
			return intField;
		}
		
		public void setIntField(int intF) {
			this.intField = intF;
		}
		
		public void setTField(T tF) {
			this.tField = tF;
		}
		
		public T getTField() {
			return tField;
		}
		
		@Override
		public String toString() {
			return "(" + tField.toString() + "," + intField + ")";
		}
	}

	/**
	 * Method useful for suppressing sysout printing
	 */
	public static void pipeSystemOutToNull() {
		System.setOut(new PrintStream(new BlackholeOutputSteam()));
	}

	private static final class BlackholeOutputSteam extends java.io.OutputStream {
		@Override
		public void write(int b){}
	}
}