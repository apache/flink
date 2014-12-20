package flink.graphs.example.utils;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import flink.graphs.Edge;
import flink.graphs.Vertex;

public class ExampleUtils {

	@SuppressWarnings({ "serial", "unchecked", "rawtypes" })
	public static void printResult(DataSet set, String msg, ExecutionEnvironment env) {
		set.output(new PrintingOutputFormatWithMessage(msg) {
		});
	}
	
	public static class PrintingOutputFormatWithMessage<T> implements OutputFormat<T> {

		private static final long serialVersionUID = 1L;
		
		private transient PrintStream stream;
		
		private transient String prefix;
		
		private String message;
		
		// --------------------------------------------------------------------------------------------
		
		/**
		 * Instantiates a printing output format that prints to standard out.
		 */
		public PrintingOutputFormatWithMessage() {}
		
		public PrintingOutputFormatWithMessage(String msg) {
			this.message = msg;	
		}

		@Override
		public void open(int taskNumber, int numTasks) {
			// get the target stream
			this.stream = System.out;
			
			// set the prefix to message
			this.prefix = message + ": ";
		}

		@Override
		public void writeRecord(T record) {
			if (this.prefix != null) {
				this.stream.println(this.prefix + record.toString());
			}
			else {
				this.stream.println(record.toString());
			}
		}

		@Override
		public void close() {
			this.stream = null;
			this.prefix = null;
		}
		
		@Override
		public String toString() {
			return "Print to System.out";
		}

		@Override
		public void configure(Configuration parameters) {}
	}

	@SuppressWarnings("serial")
	public static DataSet<Vertex<Long, NullValue>> getVertexIds(ExecutionEnvironment env,
			final long numVertices) {
        return env.generateSequence(1, numVertices)
                .map(new MapFunction<Long, Vertex<Long, NullValue>>() {
                    public Vertex<Long, NullValue> map(Long l) {
                        return new Vertex<Long, NullValue>(l, NullValue.getInstance());
                    }
                });
	}

	@SuppressWarnings("serial")
	public static DataSet<Edge<Long, NullValue>> getRandomEdges(ExecutionEnvironment env,
			final long numVertices) {
	        return env.generateSequence(1, numVertices)
	                .flatMap(new FlatMapFunction<Long, Edge<Long, NullValue>>() {
	                    @Override
	                    public void flatMap(Long key, Collector<Edge<Long, NullValue>> out) throws Exception {
	                        int numOutEdges = (int) (Math.random() * (numVertices / 2));
	                        for (int i = 0; i < numOutEdges; i++) {
	                            long target = (long) (Math.random() * numVertices) + 1;
	                            out.collect(new Edge<Long, NullValue>(key, target, NullValue.getInstance()));
	                        }
	                    }
	                });
	}

	public static final DataSet<Vertex<Long, Double>> getLongDoubleVertexData(
			ExecutionEnvironment env) {
		List<Vertex<Long, Double>> vertices = new ArrayList<Vertex<Long, Double>>();
		vertices.add(new Vertex<Long, Double>(1L, 1.0));
		vertices.add(new Vertex<Long, Double>(2L, 2.0));
		vertices.add(new Vertex<Long, Double>(3L, 3.0));
		vertices.add(new Vertex<Long, Double>(4L, 4.0));
		vertices.add(new Vertex<Long, Double>(5L, 5.0));

		return env.fromCollection(vertices);
	}
	
	public static final DataSet<Edge<Long, Double>> getLongDoubleEdgeData(
			ExecutionEnvironment env) {
		List<Edge<Long, Double>> edges = new ArrayList<Edge<Long, Double>>();
		edges.add(new Edge<Long, Double>(1L, 2L, 12.0));
		edges.add(new Edge<Long, Double>(1L, 3L, 13.0));
		edges.add(new Edge<Long, Double>(2L, 3L, 23.0));
		edges.add(new Edge<Long, Double>(3L, 4L, 34.0));
		edges.add(new Edge<Long, Double>(3L, 5L, 35.0));
		edges.add(new Edge<Long, Double>(4L, 5L, 45.0));
		edges.add(new Edge<Long, Double>(5L, 1L, 51.0));
		
		return env.fromCollection(edges);
	}
}

