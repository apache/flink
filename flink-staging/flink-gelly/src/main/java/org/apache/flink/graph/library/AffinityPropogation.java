package org.apache.flink.graph.library;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.util.Collector;

@SuppressWarnings({ "serial"})
public class AffinityPropogation implements GraphAlgorithm<Long, Long, Double> {

	private int maxIterations;
	private Double lambda;
	public AffinityPropogation(int maxIterations, double lambda){
		this.maxIterations = maxIterations;
		this.lambda = lambda;
	}
	
	/**
	 *	For each vertex v, the value is a hashmap including [k': s(v,k'), r(v, k'), a(k',v)] 
	 * @param originalGraph
	 * @return
	 */
	private Graph<Long, HashMap<Long, Tuple3<Double, Double,Double>>, Double> transferGraph(Graph<Long, Long, Double> originalGraph){
		/*Reconstruct a graph*/
		DataSet<Vertex<Long, Long>> vertices = originalGraph.getVertices();
		DataSet<Edge<Long, Double>> edges = originalGraph.getEdges();

		/*Transfer the vertex value to a hash map, which stores the <Similarity, Responsibility, Availability> to the neigbor vertices */
		DataSet<Vertex<Long,HashMap<Long, Tuple3<Double, Double,Double>>>> newVertices = vertices.map(
				new MapFunction<Vertex<Long, Long>,Vertex<Long,HashMap<Long, Tuple3<Double, Double,Double>>>>(){
					@Override
					public Vertex<Long, HashMap<Long, Tuple3<Double, Double, Double>>> map(
							Vertex<Long, Long> v) throws Exception {
						// TODO Auto-generated method stub
						HashMap<Long, Tuple3<Double, Double, Double>> hashmap = 
								new HashMap<Long, Tuple3<Double, Double,Double>>();
						hashmap.put(v.getId(),new Tuple3<Double, Double, Double>(0.0, 0.0, 0.0));
						return new Vertex<Long, HashMap<Long, Tuple3<Double, Double, Double>>>(v.getId(),
								hashmap);
					}
				}); 
		
		/*Construct a new graph with HashMap<key, <Similarity, Responsibility, Availability>> value for each vertex*/
		Graph<Long, HashMap<Long, Tuple3<Double, Double, Double>>, Double> newGraph = 
				Graph.fromDataSet(newVertices, edges, ExecutionEnvironment.getExecutionEnvironment());

		
		/*run iteration once to get similarity */
		Graph<Long, HashMap<Long, Tuple3<Double, Double,Double>>, Double> hashMappedGraph = 
				newGraph.runVertexCentricIteration(new InitialVertexUpdater(), new InitialMessenger(), 1);
			
		return hashMappedGraph;
	}
	
	public static final class InitialVertexUpdater 
	extends VertexUpdateFunction<Long, HashMap<Long, Tuple3<Double, Double,Double>>, Tuple2<Long, Double>>{
		
		@Override
		public void updateVertex(Long vertexKey, HashMap<Long, Tuple3<Double, Double,Double>> vertexValue,
				MessageIterator<Tuple2<Long, Double>> inMessages)
				throws Exception {
			for (Tuple2<Long, Double> message: inMessages){
				/*Remove degeneracies*/
				Double newS = message.f1 + (1e-12) * 66.2 * Math.random();
				if (!vertexValue.containsValue(message.f0)){
					vertexValue.put(message.f0, new Tuple3<Double, Double,Double>(newS, 0.0, 0.0));					
				}else{
					vertexValue.get(message.f0).f0 = newS;
				}
			}
			setNewVertexValue(vertexValue);
		}
	}
	
	
	public static final class InitialMessenger
		extends MessagingFunction<Long, HashMap<Long, Tuple3<Double, Double,Double>>, Tuple2<Long, Double>, Double>{
		@Override
		public void sendMessages(Long vertexKey, HashMap<Long, Tuple3<Double, Double,Double>> vertexValue)
				throws Exception {
			for (Edge<Long, Double> e: getOutgoingEdges()){
				sendMessageTo(e.getTarget(), new Tuple2<Long, Double>(vertexKey, e.getValue()));
			}
		}
	}

	@Override
	public Graph<Long, Long, Double> run(Graph<Long, Long, Double> input) {
		/*Transfer the vertex value to HashMap<Long, Tuple3<Double, Double,Double>>*/
		Graph<Long, HashMap<Long, Tuple3<Double, Double,Double>>, Double> graph = transferGraph(input);
		
		//Run the vertex centric iteration
		Graph<Long, HashMap<Long, Tuple3<Double, Double, Double>>, Double> stableGraph = graph.runVertexCentricIteration(
				new VertexUpdater(lambda), new InformationMessenger(), maxIterations*2);
		
		//Find the centers of each vertices
		DataSet<Vertex<Long, Long>> resultSet = stableGraph.groupReduceOnNeighbors(new ExamplarSelection(), EdgeDirection.OUT);
		
		//Recover to the format of original graph
		Graph<Long, Long, Double> resultGraph = Graph.fromDataSet(resultSet, 
				input.getEdges(), ExecutionEnvironment.getExecutionEnvironment());
		return resultGraph;
	}
	public static final class ExamplarSelection implements 
	NeighborsFunctionWithVertexValue<Long, HashMap<Long, Tuple3<Double, Double, Double>>, 
	Double, Vertex<Long, Long>>{

		@Override
		public void iterateNeighbors(
				Vertex<Long, HashMap<Long, Tuple3<Double, Double, Double>>> vertex,
				Iterable<Tuple2<Edge<Long, Double>, Vertex<Long, HashMap<Long, Tuple3<Double, Double, Double>>>>> neighbors,
				Collector<Vertex<Long, Long>> out) throws Exception {
			// TODO Auto-generated method stub
			/*Get Evidence*/
			HashMap<Long, Tuple3<Double, Double, Double>> hmap = vertex.getValue();
			Double selfEvidence = hmap.get(vertex.getId()).f1 + hmap.get(vertex.getId()).f2;
			if (selfEvidence > 0){
				out.collect(new Vertex<Long, Long>(vertex.getId(), vertex.getId()));
				return;
			}else{
				Double maxSimilarity = Double.NEGATIVE_INFINITY;
				Long belongExemplar = vertex.getId();
				for (Tuple2<Edge<Long, Double>, Vertex<Long, HashMap<Long, Tuple3<Double, Double, Double>>>> neigbor: neighbors){
					Long neigborId = neigbor.f1.getId();
					HashMap<Long, Tuple3<Double, Double, Double>> neigborMap = neigbor.f1.getValue();
					Double neigborEvidence = neigborMap.get(neigborId).f1 + neigborMap.get(neigborId).f2;
					//Only the neighbor vertex with positive evidence can be possible exemplar of current vertex
					if (neigborEvidence > 0 ){
						Double neigborSimilarity = neigbor.f0.getValue();
						if (neigborSimilarity > maxSimilarity){
							belongExemplar = neigborId;
							maxSimilarity = neigborSimilarity;
						}
					}
				}
				out.collect(new Vertex<Long, Long>(vertex.getId(), belongExemplar));
				return;
			}
		}

		
	}
	
//	public static final class NeighborSelection
//		implements NeighborsFunctionWithVertexValue<Long, HashMap<Long, Tuple3<Double, Double, Double>>, Double, Vertex<Long, Long>>{
//		@Override
//		public Vertex<Long, Long> iterateNeighbors(
//				Vertex<Long, HashMap<Long, Tuple3<Double, Double, Double>>> vertex,
//				Iterable<Tuple2<Edge<Long, Double>, Vertex<Long, HashMap<Long, Tuple3<Double, Double, Double>>>>> neighbors)
//				throws Exception {
//			/*Get Evidence*/
//			HashMap<Long, Tuple3<Double, Double, Double>> hmap = vertex.getValue();
//			Double selfEvidence = hmap.get(vertex.getId()).f1 + hmap.get(vertex.getId()).f2;
//			if (selfEvidence > 0){
//				return new Vertex<Long, Long>(vertex.getId(), vertex.getId());
//			}else{
//				Double maxSimilarity = Double.NEGATIVE_INFINITY;
//				Long belongExemplar = vertex.getId();
//				for (Tuple2<Edge<Long, Double>, Vertex<Long, HashMap<Long, Tuple3<Double, Double, Double>>>> neigbor: neighbors){
//					Long neigborId = neigbor.f1.getId();
//					HashMap<Long, Tuple3<Double, Double, Double>> neigborMap = neigbor.f1.getValue();
//					Double neigborEvidence = neigborMap.get(neigborId).f1 + neigborMap.get(neigborId).f2;
//					//Only the neighbor vertex with positive evidence can be possible exemplar of current vertex
//					if (neigborEvidence > 0 ){
//						Double neigborSimilarity = neigbor.f0.getValue();
//						if (neigborSimilarity > maxSimilarity){
//							belongExemplar = neigborId;
//							maxSimilarity = neigborSimilarity;
//						}
//					}
//				}
//				return new Vertex<Long, Long>(vertex.getId(), belongExemplar);
//			}
//		}
//	}

	/***************************************************************************************************/
	/*Update r(i,k) for each vertex*/
	public static final class VertexUpdater 
		extends VertexUpdateFunction<Long, HashMap<Long, Tuple3<Double, Double,Double>>, Tuple2<Long, Double>>{
		private Double lambda;
		
		public VertexUpdater(Double lambda){
			this.lambda = lambda;
		}
		
		@Override
		public void updateVertex(Long vertexKey,
				HashMap<Long, Tuple3<Double, Double, Double>> vertexValue,
				MessageIterator<Tuple2<Long, Double>> inMessages)
				throws Exception {
			int step = getSuperstepNumber();
			/*odd step: receive neighbor responsibility and update the responsibility*/
			if (step % 2 == 1){ 
				Double selfSum = vertexValue.get(vertexKey).f0 + vertexValue.get(vertexKey).f2;
				/*The max a(v, k) + s(v, k)*/
 				Double maxSum = selfSum;
 				Long maxKey = vertexKey;
 				/*The second max a(v, k) + s(v, k)*/
 				Double secondMaxSum = Double.NEGATIVE_INFINITY;
				for (Tuple2<Long, Double> msg: inMessages){
					Long adjacentVertex = msg.f0;
					Double sum = msg.f1 + vertexValue.get(adjacentVertex).f0;
					if (sum > maxSum ){
						secondMaxSum = maxSum;
						maxSum = sum;
						maxKey = adjacentVertex;
					}else if(sum > secondMaxSum){
						secondMaxSum = sum;
					}
				}
				if (maxKey != vertexKey && selfSum > secondMaxSum){
					secondMaxSum = selfSum;
				}

				/*Update responsibility*/
				for (Entry<Long, Tuple3<Double, Double, Double>> entry: vertexValue.entrySet()){
					Double newRespons = 0.0;
					if (entry.getKey() != maxKey){
						newRespons = entry.getValue().f0 - maxSum;
					}else{
						newRespons = entry.getValue().f0 - secondMaxSum;
					}
					entry.getValue().f1 = (1 - lambda) * newRespons + lambda * entry.getValue().f1;
				}
				/*reset the hashmap of the vertex*/
				setNewVertexValue(vertexValue);
				
			}else{
				/*Odd step: receive responsibility and update availability */
				double sum = 0.0;
				
				/*msg.f1 is the received responsibility*/
				ArrayList<Tuple2<Long, Double>> allMessages = new ArrayList<Tuple2<Long, Double>>();
				for (Tuple2<Long, Double> msg: inMessages){
					Double posValue = msg.f1 > 0 ? msg.f1:0;
					sum += posValue;
					allMessages.add(new Tuple2<Long, Double>(msg.f0, posValue));
				}
				
				Double selfRespons = vertexValue.get(vertexKey).f1;
		
				for (Tuple2<Long, Double> msg: allMessages){
					Double gathered = selfRespons + sum - msg.f1;
					Double newAvailability = gathered < 0 ? gathered: 0;
					vertexValue.get(msg.f0).f2 = (1 - lambda) * newAvailability + lambda * vertexValue.get(msg.f0).f2;
				}
				/*update self-availability*/
				vertexValue.get(vertexKey).f2 = (1 - lambda) * sum  + lambda *vertexValue.get(vertexKey).f2;
				/*reset the hash map*/
				setNewVertexValue(vertexValue);
			}
			
		}
	}
	
	/*Send a(k',i) to k'*/
	public static final class InformationMessenger
	extends MessagingFunction<Long, HashMap<Long, Tuple3<Double, Double,Double>>, Tuple2<Long, Double>, Double>{
		@Override
		public void sendMessages(Long vertexKey,
				HashMap<Long, Tuple3<Double, Double, Double>> vertexValue)
				throws Exception {
			if (getSuperstepNumber() % 2 == 1){
				/*Odd step: Propagate availability*/
				for (Edge<Long, Double> e: getOutgoingEdges()){
					Long dest = e.getTarget();
					if (dest != vertexKey){
						sendMessageTo(dest, new Tuple2<Long, Double>(vertexKey, vertexValue.get(dest).f2));
					}
				}
			}else{
				/*Even step: propagate responsibility*/
				for (Edge<Long, Double> e: getOutgoingEdges()){
					Long dest = e.getTarget();
					if (dest != vertexKey)
						sendMessageTo(dest, new Tuple2<Long, Double>(vertexKey, vertexValue.get(dest).f1));
				}
			}
				
		}
	}
	
}
