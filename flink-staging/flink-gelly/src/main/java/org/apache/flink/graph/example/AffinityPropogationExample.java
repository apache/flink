package org.apache.flink.graph.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.AffinityPropogationData;
import org.apache.flink.graph.library.AffinityPropogation;
import org.apache.flink.util.Collector;

public class AffinityPropogationExample implements ProgramDescription{

	public static String vertexPreferenceInputPath = null;
	public static String similarGraphInputPath = null;
	public static String outputPath = null;
	public static int maxIterations = 100;
	public static double lambda = 0.5;
	public static boolean fileOutput = false;
	
	@Override
	public String getDescription() {
		// TODO Auto-generated method stub
		return "This is an example of Affinity Propogation";
	}
	
	private static boolean parseParameters(String[] programArguments) {
		if(programArguments.length > 0) {
			// parse input arguments
			fileOutput = true;
			if(programArguments.length == 5) {
				similarGraphInputPath = programArguments[0];
				vertexPreferenceInputPath = programArguments[1];
				outputPath = programArguments[2];
				maxIterations = Integer.parseInt(programArguments[3]);
				lambda = Double.parseDouble(programArguments[4]);
			} else {
				System.err.println("Usage: AffinityPropogation <similarity graph path> <vertex preference path> "
						+ "<result path> <num of iterations> <lambda>");
				return false;
			}
		} else {
			System.out.println("Executing Affinity propogation example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage:  AffinityPropogation <similarity graph path> <vertex preference path> <result path> <num of iterations> <lambda>");
		}
		return true;
	}


	public static void main(String[] args) throws Exception{
		if(!parseParameters(args)) {
			return;
		}
		/*set up environment*/
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		/*Set up graph*/
		DataSet<Vertex<Long, Long>> vertices = getVertexDataSet(env);
		DataSet<Edge<Long, Double>> distanceEdges = getEdgeDataSet(env);
		DataSet<Edge<Long, Double>> selfEdges = getSelfEdgeSet(env);
		
		DataSet<Edge<Long, Double>> allEdges = distanceEdges.union(selfEdges);
		
		Graph<Long, Long, Double> graph = Graph.fromDataSet(vertices, allEdges, env);		
		/*Run affinity propagation algorithm*/
		Graph<Long, Long, Double> result = graph.run(new AffinityPropogation(maxIterations, lambda));
		
		// emit result
		if (fileOutput){
			result.getVertices().writeAsCsv(outputPath,"\n", " ");
		}else{
			result.getVertices().print();
		}
		env.execute("Affinity Propogation Example");
	}
	
	@SuppressWarnings("serial")
	public static DataSet<Vertex<Long, Long>> getVertexDataSet(ExecutionEnvironment env){
		if (fileOutput){
			return env.readCsvFile(vertexPreferenceInputPath).
					fieldDelimiter(' ').lineDelimiter("\n")
					.types(Long.class, Double.class).
					map(new MapFunction<Tuple2<Long, Double>, Vertex<Long, Long>>() {
						@Override
						public Vertex<Long, Long> map(Tuple2<Long, Double> tuple2) throws Exception {
							return new Vertex<Long, Long>(tuple2.f0, tuple2.f0);
						}
					});
		}else{
			return AffinityPropogationData.getDefautVertexSet(env);
		}
	}
	@SuppressWarnings("serial")
	public static DataSet<Edge<Long, Double>> getEdgeDataSet(ExecutionEnvironment env){
		if (fileOutput){
			return env.readCsvFile(similarGraphInputPath)
					.fieldDelimiter(' ').lineDelimiter("\n")
					.types(Long.class, Long.class, Double.class)
					.map(new MapFunction<Tuple3<Long, Long, Double>, Edge<Long, Double>>(){
						@Override
						public Edge<Long, Double> map(
								Tuple3<Long, Long, Double> tuple3) throws Exception {
							// TODO Auto-generated method stub
							return new Edge<Long, Double>(tuple3.f0, tuple3.f1,tuple3.f2);
						}
						
					});
		}else{
			return AffinityPropogationData.getDefautEdgeDataSet(env);
		}
	}
	
	@SuppressWarnings("serial")
	public static DataSet<Edge<Long, Double>> getSelfEdgeSet(ExecutionEnvironment env){
		if (fileOutput){
			return env.readCsvFile(vertexPreferenceInputPath)
					.fieldDelimiter(' ').lineDelimiter("\n")
					.types(Long.class, Double.class)
					.map(new MapFunction<Tuple2<Long, Double>, Edge<Long, Double>>(){
						@Override
						public Edge<Long, Double> map(Tuple2<Long, Double> value)
								throws Exception {
							return new Edge<Long, Double>(value.f0, value.f0, value.f1);
						}
						
					});
		}else{
			return AffinityPropogationData.getDefautSelfEdgeDataSet(env);
		}
	}
	
}
