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

package org.apache.flink.graph.library;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgesFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;


/**
 * This is an implementation of the Binary Affinity Propagation algorithm using a scatter-gather iteration.
 * Note that is not the original Affinity Propagation.
 *
 * The input is an undirected graph where the vertices are the points to be clustered and the edge weights are the
 * similarities of these points among them.
 *
 * The output is a Dataset of Tuple2, where f0 is the point id and f1 is the exemplar, so the clusters will be the
 * the Tuples grouped by f1
 *
 * @see <a href="http://www.psi.toronto.edu/pubs2/2009/NC09%20-%20SimpleAP.pdf">
 */

@SuppressWarnings("serial")
public class AffinityPropagation implements GraphAlgorithm<Long,NullValue,Double,DataSet<Tuple2<Long, Long>>> {

	private static Integer maxIterations;
	private static float damping;
	private static float epsilon;

	/**
	 * Creates a new AffinityPropagation instance algorithm instance.
	 *
	 * @param maxIterations The maximum number of iterations to run
	 * @param damping Damping factor.
	 * @param epsilon Epsilon factor. Do not send message to a neighbor if the new message
	 * has not changed more than epsilon.
	 */
	public AffinityPropagation(Integer maxIterations, float damping, float epsilon) {
		this.maxIterations = maxIterations;
		this.damping = damping;
		this.epsilon = epsilon;
	}

	@Override
	public DataSet<Tuple2<Long, Long>> run(Graph<Long, NullValue, Double> input) throws Exception {

		// Create E and I AP vertices
		DataSet<Vertex<Long, APVertexValue>> verticesWithAllInNeighbors =
			input.groupReduceOnEdges(new InitAPVertex(), EdgeDirection.IN);

		List<Vertex<Long, APVertexValue>> APvertices = verticesWithAllInNeighbors.collect();

		// Create E and I AP edges. Could this be done with some gelly functionality?
		List<Edge<Long, NullValue>> APedges = new ArrayList<>();

		for(int i = 1; i < input.numberOfVertices() + 1; i++){
			for(int j = 1; j < input.numberOfVertices() + 1; j++){
				APedges.add(new Edge<>(i * 10L, j * 10L + 1, NullValue.getInstance()));
			}
		}

		DataSet<Edge<Long, NullValue>> APEdgesDS = input.getContext().fromCollection(APedges);
		DataSet<Vertex<Long, APVertexValue>> APVerticesDS = input.getContext().fromCollection(APvertices);

		ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
		parameters.registerAggregator("convergedAggregator", new LongSumAggregator());

		Graph<Long, APVertexValue, NullValue> APgraph
			= Graph.fromDataSet(APVerticesDS, APEdgesDS, input.getContext());

		return APgraph.getUndirected().runScatterGatherIteration(new APVertexUpdater(input.numberOfVertices() * 2),
			new APMessenger(),this.maxIterations,parameters).getVertices().filter(new FilterFunction<Vertex<Long, APVertexValue>>() {
			@Override
			public boolean filter(Vertex<Long, APVertexValue> vertex) throws Exception {
				return vertex.getId()%2 == 0;
			}
		}).map(new MapFunction<Vertex<Long, APVertexValue>, Tuple2<Long, Long>>() {
			@Override
			public Tuple2<Long, Long> map(Vertex<Long, APVertexValue> value) throws Exception {
				Tuple2<Long, Long> returnValue = new Tuple2<>(value.getId()/10, value.getValue().getExemplar()/10);
				return returnValue;
			}
		});

	}

	/**
	* Foreach input point we have to create a pair of E,I vertices. Same structure is used for both vertex type, to
	* diferenciate E and I vertices is used the id. Foreach input point we will create:
	*
	* - One E vertex with the id as the original input id * 10 + 1
	* - One I vertex with the id as the original input id * 10
	*
	* This way even ids are from E type vertices and odd ids are from I vertices.
	*
	* It also calculates adds the weights to the I vertices. Notice that the S vertices are not created and the weights
	* are added to the I vertices, simulating the S vertex.
	*/

	@SuppressWarnings("serial")
	private static final class InitAPVertex implements EdgesFunction<Long, Double, Vertex<Long, APVertexValue>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Double>>> edges,
								Collector<Vertex<Long, APVertexValue>> out) throws Exception {

			Vertex<Long, APVertexValue> APvertexI = new Vertex<>();
			Vertex<Long, APVertexValue> APvertexE = new Vertex<>();

			Iterator<Tuple2<Long, Edge<Long, Double>>> itr = edges.iterator();
			Tuple2<Long, Edge<Long, Double>> edge = itr.next();

			APvertexE.setId(edge.f0 * 10 + 1);
			APvertexE.setValue(new APVertexValue());

			APvertexI.setId(edge.f0 * 10);
			APvertexI.setValue(new APVertexValue());
			APvertexI.getValue().getWeights().put(edge.f1.getSource() * 10 + 1, edge.f1.getValue());

			APvertexE.getValue().getOldValues().put(edge.f1.getSource() * 10, 0.0);
			APvertexI.getValue().getOldValues().put(edge.f1.getSource() * 10 + 1, 0.0);


			while(itr.hasNext()){
				edge = itr.next();
				APvertexI.getValue().getWeights().put(edge.f1.getSource() * 10 + 1, edge.f1.getValue());

				APvertexE.getValue().getOldValues().put(edge.f1.getSource() * 10, 0.0);
				APvertexI.getValue().getOldValues().put(edge.f1.getSource() * 10 + 1, 0.0);

			}

			out.collect(APvertexE);
			out.collect(APvertexI);
		}
	}

	/**
	 * Vertex updater
	 */

	@SuppressWarnings("serial")
	public static final class APVertexUpdater extends VertexUpdateFunction<Long, APVertexValue, APMessage> {

		private Long numOfVertex;
		LongSumAggregator aggregator = new LongSumAggregator();

		public APVertexUpdater(Long numOfVertex){
			this.numOfVertex = numOfVertex;
		}

		@Override
		public void preSuperstep() throws Exception {

			aggregator = getIterationAggregator("convergedAggregator");

		}

		/**
		 * Main vertex update function. It calls updateIVertex, updateEVertex, computeExemplars or computeClusters
		 * depending on the phase of the algorithm execution
		 */

		@Override
		public void updateVertex(Vertex<Long, APVertexValue> vertex,
								MessageIterator<APMessage> inMessages) {

			//If all vertices converged compute the Exemplars

			if(getSuperstepNumber() > 1
				&& (((LongValue)getPreviousIterationAggregate("convergedAggregator")).getValue()
				== numOfVertex|| getSuperstepNumber() == maxIterations-2)) {
				computeExemplars(vertex, inMessages);
				return;
			}

			//Once the exemplars have been calculated calculate the clusters. The aggregator has a negative value assigned
			//when exemplars are calculated
			if(getSuperstepNumber() > 1
				&& ((LongValue)getPreviousIterationAggregate("convergedAggregator")).getValue()
				< 0) {
				if(vertex.getValue().getExemplar() < 0){
					computeClusters(vertex, inMessages);
				}
				return;
			}

			//Call updateIvertex or updateEvertex depending on the id
			if(vertex.getId()%2 == 0){
				updateIVertex(vertex, inMessages);
			}else{
				updateEVertex(vertex, inMessages);
			}


		}

		/**
		 * I vertices calculations
         */

		private void updateIVertex(Vertex<Long, APVertexValue> vertex,
								MessageIterator<APMessage> inMessages){

			double bestValue = Double.NEGATIVE_INFINITY, secondBestValue = Double.NEGATIVE_INFINITY;
			Long bestNeighbour = null;

			List<APMessage> cache = new ArrayList<>();

			while(inMessages.hasNext()){
				APMessage message = inMessages.next();
				cache.add(message);
				Double weight = vertex.getValue().getWeights().get(message.getFrom());
				if(weight +  message.getMessageValue() > bestValue){
					secondBestValue = bestValue;
					bestValue = weight +  message.getMessageValue();
					bestNeighbour = message.getFrom();
				}else if(weight +  message.getMessageValue() > secondBestValue){
					secondBestValue = weight +  message.getMessageValue();
				}
			}

			boolean converged = true;

			for(APMessage message:cache){
				double value;

				if(message.getFrom()/10 == bestNeighbour/10){
					value = -secondBestValue;
				}else{
					value = -bestValue;
				}

				value = value + vertex.getValue().getWeights().get(message.getFrom());

				Double oldValue = vertex.getValue().getOldValues().get(message.getFrom());

				if(Math.abs(oldValue - value) < epsilon) {
					vertex.getValue().getValues().put(message.getFrom(),value);
				}else{
					value = damping * oldValue + (1 - damping) * value;
					vertex.getValue().getValues().put(message.getFrom(),value);
					vertex.getValue().getOldValues().put(message.getFrom(),value);
					converged = false;
				}
			}

			if(converged){
				aggregator.aggregate(1);
			}

			setNewVertexValue(vertex.getValue());

		}

		/**
		 * E vertices calculations
		 */

		private void updateEVertex(Vertex<Long, APVertexValue> vertex,
								MessageIterator<APMessage> inMessages){

			double sum = 0;
			double exemplarMessage = 0;

			List<APMessage> cache = new ArrayList<>();

			while(inMessages.hasNext()){
				APMessage message = inMessages.next();
				cache.add(message);
				if(vertex.getId()/10 != message.getFrom()/10){
					sum += Math.max(0,message.getMessageValue());
				}else{
					exemplarMessage = message.getMessageValue();
				}

			}

			boolean converged = true;

			for(APMessage message:cache){
				double value;
				if(vertex.getId()/10 == message.getFrom()/10){
					value = sum;
				}else{
					double a = exemplarMessage + sum - Math.max(message.getMessageValue(), 0);
					value = Math.min(0, a);
				}

				Double oldValue = vertex.getValue().getOldValues().get(message.getFrom());

				if(Math.abs(oldValue - value) < epsilon) {
					vertex.getValue().getValues().put(message.getFrom(),value);
				}else{
					value = damping * oldValue + (1 - damping) * value;
					vertex.getValue().getValues().put(message.getFrom(),value);
					vertex.getValue().getOldValues().put(message.getFrom(),value);
					converged = false;
				}
			}

			if(converged){
				aggregator.aggregate(1);
			}

			setNewVertexValue(vertex.getValue());

		}

		/**
		 * Computes exemplars
		 */

		private void computeExemplars(Vertex<Long, APVertexValue> vertex, MessageIterator<APMessage> inMessages){

			aggregator.aggregate(1);

			for(Long key : vertex.getValue().getValues().keySet()){
				vertex.getValue().getValues().put(key, 0.0);
			}

			if(vertex.getId()%2 != 0){
				while(inMessages.hasNext()){
					APMessage message = inMessages.next();
					if (message.getFrom()/10 == vertex.getId()/10) {
						double lastMessageValue = vertex.getValue().getOldValues().get(message.getFrom());
						double messageValue = message.getMessageValue();
						double belief = messageValue + lastMessageValue;
						if (belief >= 0) {
							for(Long key : vertex.getValue().getValues().keySet()){
								vertex.getValue().getValues().put(key, 1.0);
							}
						}
					}
				}
			}

			aggregator.aggregate(-2);
			setNewVertexValue(vertex.getValue());
		}

		/**
		 * Computes clusters
		 */

		private void computeClusters(Vertex<Long, APVertexValue> vertex, MessageIterator<APMessage> inMessages){

			if(vertex.getId()%2 != 0){
				return;
			}

			long bestExemplar = -1;
			double maxValue = Double.NEGATIVE_INFINITY;

			while(inMessages.hasNext()){

				APMessage message = inMessages.next();
				final long exemplar = message.getFrom();

				if(message.getMessageValue() == 1){

					if(message.getFrom()/10 == vertex.getId()/10) {
						bestExemplar = -1;
						break;
					}

					double value = vertex.getValue().getWeights().get(message.getFrom());
					if (value > maxValue) {
						maxValue = value;
						bestExemplar = exemplar;
					}
				}
			}

			if(bestExemplar == -1){
				vertex.getValue().setExemplar(vertex.getId());
			}else{
				vertex.getValue().setExemplar(bestExemplar);
			}

			aggregator.aggregate(-2);
			setNewVertexValue(vertex.getValue());

		}

	}

	/**
	 * Messaging function
	 */

	@SuppressWarnings("serial")
	public static final class APMessenger extends MessagingFunction<Long, APVertexValue, APMessage, NullValue> {

		@Override
		public void sendMessages(Vertex<Long, APVertexValue> vertex) {

			if(vertex.getValue().getValues().size() == 0){
				APMessage message = new APMessage();
				message.setMessageValue(0);
				message.setFrom(vertex.getId());
				sendMessageToAllNeighbors(message);
			}

			for (Map.Entry<Long, Double> entry : vertex.getValue().getValues().entrySet()) {
				Long key = entry.getKey();
				Double value = entry.getValue();
				APMessage message = new APMessage();
				message.setFrom(vertex.getId());
				message.setMessageValue(value);
				sendMessageTo(key,message);
			}
		}
	}

	/**
	 * Vertex value class. It contains:
	 * - weights: for I vertices is filled with the similarity with its neighbours
	 * - values: values to be sent
	 * - oldValues: previous values sent, used to calculate the damping
	 * - exemplar: exemplar selected by this point
	 */

	public static final class APVertexValue{

		private HashMap<Long,Double> weights;
		private HashMap<Long,Double> values;
		private HashMap<Long,Double> oldValues;
		private long exemplar;

		public APVertexValue(){
			this.weights = new HashMap<>();
			this.values = new HashMap<>();
			this.oldValues = new HashMap<>();
			this.exemplar = -1L;
		}

		public HashMap<Long, Double> getValues() {
			return values;
		}

		public void setValues(HashMap<Long, Double> values) {
			this.values = values;
		}

		public HashMap<Long, Double> getWeights() {
			return weights;
		}

		public void setWeights(HashMap<Long, Double> weights) {
			this.weights = weights;
		}

		public HashMap<Long, Double> getOldValues() {
			return oldValues;
		}

		public void setOldValues(HashMap<Long, Double> oldValues) {
			this.oldValues = oldValues;
		}

		public long getExemplar() {
			return exemplar;
		}

		public void setExemplar(long exemplar) {
			this.exemplar = exemplar;
		}
	}

	/**
	 * Message sent:
	 * - from: sender
	 * - messageValue: value to be sent
	*/

	public static final class APMessage{

		private Long from;
		private double messageValue;

		public double getMessageValue() {
			return messageValue;
		}

		public void setMessageValue(double messageValue) {
			this.messageValue = messageValue;
		}

		public Long getFrom() {
			return from;
		}

		public void setFrom(Long from) {
			this.from = from;
		}
	}
}
