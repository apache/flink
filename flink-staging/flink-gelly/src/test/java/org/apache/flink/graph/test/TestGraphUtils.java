package flink.graphs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class TestGraphUtils {

	public static final DataSet<Vertex<Long, Long>> getLongLongVertexData(
			ExecutionEnvironment env) {
		List<Vertex<Long, Long>> vertices = new ArrayList<Vertex<Long, Long>>();
		vertices.add(new Vertex<Long, Long>(1L, 1L));
		vertices.add(new Vertex<Long, Long>(2L, 2L));
		vertices.add(new Vertex<Long, Long>(3L, 3L));
		vertices.add(new Vertex<Long, Long>(4L, 4L));
		vertices.add(new Vertex<Long, Long>(5L, 5L));
		
		return env.fromCollection(vertices);
	}
	
	public static final DataSet<Edge<Long, Long>> getLongLongEdgeData(
			ExecutionEnvironment env) {
		List<Edge<Long, Long>> edges = new ArrayList<Edge<Long, Long>>();
		edges.add(new Edge<Long, Long>(1L, 2L, 12L));
		edges.add(new Edge<Long, Long>(1L, 3L, 13L));
		edges.add(new Edge<Long, Long>(2L, 3L, 23L));
		edges.add(new Edge<Long, Long>(3L, 4L, 34L));
		edges.add(new Edge<Long, Long>(3L, 5L, 35L));
		edges.add(new Edge<Long, Long>(4L, 5L, 45L));
		edges.add(new Edge<Long, Long>(5L, 1L, 51L));
		
		return env.fromCollection(edges);
	}

	/**
	 * A graph with invalid vertex ids
	 */
	public static final DataSet<Vertex<Long, Long>> getLongLongInvalidVertexData(
			ExecutionEnvironment env) {
		List<Vertex<Long, Long>> vertices = new ArrayList<Vertex<Long, Long>>();
		vertices.add(new Vertex<Long, Long>(15L, 1L));
		vertices.add(new Vertex<Long, Long>(2L, 2L));
		vertices.add(new Vertex<Long, Long>(3L, 3L));
		vertices.add(new Vertex<Long, Long>(4L, 4L));
		vertices.add(new Vertex<Long, Long>(5L, 5L));

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
	public static final List<Vertex<Long, Long>> getLongLongVertices(
			ExecutionEnvironment env) {
		List<Vertex<Long, Long>> vertices = new ArrayList<>();
		vertices.add(new Vertex<Long, Long>(1L, 1L));
		vertices.add(new Vertex<Long, Long>(2L, 2L));
		vertices.add(new Vertex<Long, Long>(3L, 3L));
		vertices.add(new Vertex<Long, Long>(4L, 4L));
		vertices.add(new Vertex<Long, Long>(5L, 5L));

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
	public static final List<Edge<Long, Long>> getLongLongEdges(
			ExecutionEnvironment env) {
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
}