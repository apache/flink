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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.graph.pregel.VertexCentricConfiguration;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;


/**
 * This is an implementation of the Binary Affinity Propagation algorithm using the vertex centric iteration model.
 * Note that is not the original Affinity Propagation.
 *
 * The input is a 2D square array of dobules being the similarities between the points to be clustered.
 *
 * The output is a Dataset of Tuple2, where f0 is the point id and f1 is the exemplar. Clusters will be the tuples
 * grouped by f1
 *
 * When calling the constructor with the damping factor it will damp all messages and the convergence will depend on the
 * message values. When calling the constructor with the convergence factor, messages will not be damped and convergence
 * will depend on the local changes of the exemplars
 *
 * @see <a href="http://www.psi.toronto.edu/pubs2/2009/NC09%20-%20SimpleAP.pdf">
 * @see <a href="http://www.psi.toronto.edu/affinitypropagation/FreyDueckScience07.pdf">
 */

@SuppressWarnings("serial")
public class AffinityPropagation implements GraphAlgorithm<Long, AffinityPropagation.APVertexValue,
	NullValue,DataSet<Tuple2<Long, Long>>> {

	private static Integer maxIterations;
	private static float damping;
	private static float epsilon;
	private static int convergenceFactor;

	/**
	 * Creates a new AffinityPropagation algorithm instance with no damping. This instance will converge when no changes
	 * in the exemplars happen during @param convergenceFactor times
	 *
	 * @param maxIterations The maximum number of iterations
	 * @param convergenceFactor Number of iterations that exemplars need to remain the same to converge
	 */
	public AffinityPropagation(Integer maxIterations, int convergenceFactor) {
		this.maxIterations = maxIterations;
		this.damping = 0;
		this.convergenceFactor = convergenceFactor;
	}

	/**
	 * Creates a new AffinityPropagation algorithm instance with damping. This instance will converge when all messages
	 * in all vertices do not change more than @param epsilon app applying a @param damping factor on each iteration
	 *
	 * @param maxIterations The maximum number of iterations
	 * @param damping Damping factor
	 * @param epsilon Do not send message to a neighbor if the new message has not changed more than epsilon
	 */
	public AffinityPropagation(Integer maxIterations, float damping, float epsilon) {
		this.maxIterations = maxIterations;
		this.damping = damping;
		this.epsilon = epsilon;
		this.convergenceFactor = 0;
	}

	/*
	* Function to create a Binary Affinity Propagation graph from the input similarity matrix. Matrix size has to be
	* n x n. It will create n E type vertices and n I type vertices. It also creates the edges between the vertices.
	* This graph will be used to run the algorithm. It will assign even IDs for I vertices and odd IDs for E vertices
	*/
	public Graph<Long, APVertexValue, NullValue> createAPGraph(double[][] matrix, ExecutionEnvironment env){

		List<Vertex<Long, APVertexValue>> IVerticesList = new ArrayList<Vertex<Long, APVertexValue>>();
		List<Vertex<Long, APVertexValue>> EVerticesList = new ArrayList<Vertex<Long, APVertexValue>>();
		List<Edge<Long, NullValue>> EdgesList = new ArrayList<Edge<Long, NullValue>>();

		int size = 0;

		if((size = matrix.length) != matrix[0].length){
			return null;
		}

		/*
		* Loop to create the E and I vertices
		*/
		for (int i = 0; i< size; i++) {

			Vertex<Long, APVertexValue> APvertexI = new Vertex<>();
			Vertex<Long, APVertexValue> APvertexE = new Vertex<>();

			APvertexE.setId((long)i * 10 + 1);
			APvertexE.setValue(new APVertexValue());

			APvertexI.setId((long)i * 10);
			APvertexI.setValue(new APVertexValue());

			EVerticesList.add(APvertexE);
			IVerticesList.add(APvertexI);
		}

		/*
		* Loop to create the edges and create the lists with the old sent values if damping is different to 0, it also
		* populates the similarities list at I vertices
		*/
		for(int i = 0; i < size; i++){

			Vertex<Long, APVertexValue> APvertexI = IVerticesList.get(i);
			Vertex<Long, APVertexValue> APvertexE = EVerticesList.get(i);

			for (int j = 0; j< matrix[i].length; j++){

				APvertexI.getValue().getWeights().put((long)j * 10 + 1, matrix[i][j]);

				if(damping != 0) {
					APvertexE.getValue().getOldValues().put((long) j * 10, 0.0);
					APvertexI.getValue().getOldValues().put((long) j * 10 + 1, 0.0);
				}

				EdgesList.add(new Edge<>(i * 10L, j * 10L + 1, NullValue.getInstance()));

			}
		}

		List<Vertex<Long, APVertexValue>> AllVerticesList =  new ArrayList<Vertex<Long, APVertexValue>>();
		AllVerticesList.addAll(EVerticesList);
		AllVerticesList.addAll(IVerticesList);

		DataSet<Vertex<Long, APVertexValue>> VerticesDataSet = env.fromCollection(AllVerticesList);
		DataSet<Edge<Long, NullValue>> EdgesDataSet = env.fromCollection(EdgesList);

		Graph<Long, APVertexValue, NullValue> APgraph
			= Graph.fromDataSet(VerticesDataSet, EdgesDataSet, env);

		return APgraph;
	}

	/*
	* Execute the algorithm and return the tuples of each point and its exemplar.
	*/
	@Override
	public DataSet<Tuple2<Long, Long>> run(Graph<Long, APVertexValue, NullValue> input) throws Exception {

		VertexCentricConfiguration parameters = new VertexCentricConfiguration();
		parameters.registerAggregator("convergedAggregator", new LongSumAggregator());

		return input.getUndirected().runVertexCentricIteration(new APVertexUpdater(input.numberOfVertices(),
				this.convergenceFactor), null, this.maxIterations, parameters).getVertices().filter(new FilterFunction<Vertex<Long, APVertexValue>>() {
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
	 * Vertex updater
	 */
	public static final class APVertexUpdater
		extends org.apache.flink.graph.pregel.ComputeFunction<Long, APVertexValue, NullValue, APMessage> {

		private Long numOfVertex;
		private int convergenceFactor;
		private LongSumAggregator aggregator = new LongSumAggregator();


		/**
		 * Creates a new APVertexUpdater instance
		 * @param numOfVertex Number of AP vertices, that will be the number of points to cluster x 2
		 * @param convergenceFactor Convergence factor to be used if damping has a value of 0
		 */
		public APVertexUpdater(Long numOfVertex, int convergenceFactor){
			this.numOfVertex = numOfVertex;
			this.convergenceFactor = convergenceFactor;
		}

		@Override
		public void preSuperstep() throws Exception {

			aggregator = getIterationAggregator("convergedAggregator");

			System.out.println("Superstep: " + getSuperstepNumber());
			if(getSuperstepNumber() > 1) {
				System.out.println("Aggregator: " + ((LongValue)getPreviousIterationAggregate("convergedAggregator")).getValue());
			}

		}

		//@Override
		public void compute(Vertex<Long, APVertexValue> vertex,
							MessageIterator<APMessage> inMessages) throws Exception {

			/*
			* First step is initializing the algorithm sending a 0 valued message to all vertices
			*/
			if(getSuperstepNumber() == 1){
				APMessage message = new APMessage();
				message.setMessageValue(0);
				message.setFrom(vertex.getId());
				sendMessageToAllNeighbors(message);
				setNewVertexValue(vertex.getValue());
				return;
			}

			/*
			* Compute the exemplars values when the algorithm finishes the computations. This will happen if all vertices
			* have converged or if the maximum number of iterations has been reached
			*/
			if(getSuperstepNumber() > 1
				&& (((LongValue)getPreviousIterationAggregate("convergedAggregator")).getValue()
				== numOfVertex || getSuperstepNumber() == maxIterations - 2)) {
				computeExemplars(vertex, inMessages);
				return;
			}

			/*
			* Last step of the execution, once the exemplars have been set it will create the clusters. This condition
			* will be satisfied when the aggregator has a negative value. This happens after the exemplars have been
			* selected at previous superstep
			*/
			if(getSuperstepNumber() > 2 &&
				((LongValue)getPreviousIterationAggregate("convergedAggregator")).getValue() < 0) {
				if(vertex.getValue().getExemplar() < 0){
					computeClusters(vertex, inMessages);
				}
				return;
			}

			/*
			* Do the computations and update the vertices. Depending on the vertex type (even IDs for I vertices and
			* odd IDs for E vertices) its correspondent function is called
			*/
			if(vertex.getId()%2 == 0){
				updateIVertex(vertex, inMessages);
			}else{
				updateEVertex(vertex, inMessages);
			}

		}

		/*
		* Compute messages to be sent and updates I vertices. Computations can be found in the Binary Affinity
		* propagation paper. Convergence of I vertices will depend if damping is used
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

				if(damping == 0){
					converged = computeConvergenceFactor(vertex, message, value) && converged;
					sendNonDampedMessage(vertex, value, message.getFrom());
				}else{
					converged = sendDampedMessage(vertex, value, message.getFrom()) && converged;
				}

			}

			if(converged){
				aggregator.aggregate(1);
			}

			setNewVertexValue(vertex.getValue());

		}

		/*
		* Function to compute the convergence of the I vertices if a 0 damping value is used. It will converge and
		* return true if the I vertex exemplar value has remained the same at least for the convergenceFactor times,
		* otherwise will return false.
		*/
		public boolean computeConvergenceFactor(Vertex<Long, APVertexValue> vertex, APMessage message, double value){
			if (message.getFrom()/10 == vertex.getId()/10) {
				double messageValue = message.getMessageValue();
				double belief = messageValue + value;
				if (belief >= 0) {
					if(vertex.getValue().getExemplar() == 1){
						vertex.getValue().setConvergenceFactorCounter(vertex.getValue().
							getConvergenceFactorCounter() + 1);
					}else{
						vertex.getValue().setExemplar(1L);
						vertex.getValue().setConvergenceFactorCounter(0);
					}
				}else{
					if(vertex.getValue().getExemplar() == 1){
						vertex.getValue().setExemplar(0L);
						vertex.getValue().setConvergenceFactorCounter(0);
					}else{
						vertex.getValue().setConvergenceFactorCounter(vertex.getValue().
							getConvergenceFactorCounter() + 1);
					}
				}
			}

			return (convergenceFactor <= vertex.getValue().getConvergenceFactorCounter());
		}

		/*
		* Compute messages to be sent and updates E vertices. Computations can be found in the Binary Affinity
		* propagation paper. If a 0 damping value is used E vertices will always be converged, otherwise it will depend
		* on the messages.
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

				if(damping == 0){
					converged = true;
					sendNonDampedMessage(vertex, value, message.getFrom());
				}else{
					converged = sendDampedMessage(vertex, value, message.getFrom()) && converged;
				}

			}

			if(converged){
				aggregator.aggregate(1);
			}

			setNewVertexValue(vertex.getValue());

		}

		/**
		 * Function to send messages for non damping execution. It creates and send the messages
		 */
		private void sendNonDampedMessage(Vertex<Long, APVertexValue> vertex, double value, Long from){

			boolean converged = true;

			APMessage toSend = new APMessage();
			toSend.setFrom(vertex.getId());
			toSend.setMessageValue(value);

			if(vertex.getId()%2 == 0){
				System.out.println("(I,"+ vertex.getId()/10 +") -> (E,"+ from/10 +") : " + value);
			}else{
				System.out.println("(E,"+ vertex.getId()/10 +") -> (I,"+ from/10 +") : " + value);
			}

			sendMessageTo(from, toSend);

		}

		/**
		 * Function to send messages for damping execution. It creates and send the messages. It also returns if that
		 * message has converged
		 */
		private boolean sendDampedMessage(Vertex<Long, APVertexValue> vertex, double value, Long from){

			boolean converged = true;

			Double oldValue = vertex.getValue().getOldValues().get(from);

			APMessage toSend = new APMessage();
			toSend.setFrom(vertex.getId());
			toSend.setMessageValue(value);

			if(Math.abs(oldValue - value) < epsilon) {
				toSend.setMessageValue(value);
			}else{
				value = damping * oldValue + (1 - damping) * value;
				vertex.getValue().getOldValues().put(from, value);
				toSend.setMessageValue(value);
				converged = false;
			}

			if(vertex.getId()%2 == 0){
				System.out.println("(I,"+ vertex.getId()/10 +") -> (E,"+ from/10 +") : " + value);
			}else{
				System.out.println("(E,"+ vertex.getId()/10 +") -> (I,"+ from/10 +") : " + value);
			}

			sendMessageTo(from, toSend);

			return converged;
		}

		/**
		 * Function that computes the exemplars. It will send a message with 1 value in case that vertex is an exemplar.
		 * It will use the aggregator to maintain the state and go to the next step of the algorithm that is creating
		 * the clusters. This is done setting it to a negative value.
		 * To compute the exemplars we recover the hidden values of c_jj variables (those ones in the diagonal). To do
		 * this we have to add all the incoming messages of c_jj, that is the p_ij+a_ij where p_ij=s_ij+n_ij in the E
		 * vertices
		 */
		private void computeExemplars(Vertex<Long, APVertexValue> vertex, MessageIterator<APMessage> inMessages){

			aggregator.aggregate(1);

			vertex.getValue().setExemplar(-1L);

			APMessage toSend = new APMessage();
			toSend.setFrom(vertex.getId());
			toSend.setMessageValue(0.0);

			/*
			* For E vertices
			*/
			if(vertex.getId()%2 != 0){
				while(inMessages.hasNext()){

					double sum = 0;
					double exemplarMessage = 0;
					double value = 0;

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

					for(APMessage message:cache){
						if(vertex.getId()/10 == message.getFrom()/10){
							value = sum;
						}else{
							double a = exemplarMessage + sum - Math.max(message.getMessageValue(), 0);
							value = Math.min(0, a);
						}

						/*
						* Those in the diagonal
						*/
						if (message.getFrom()/10 == vertex.getId()/10) {
							double messageValue = message.getMessageValue();
							double belief = messageValue + value;
							/*
							* If belief > 0 means this is an exemplar
							*/
							if (belief >= 0) {
								toSend.setMessageValue(1.0);
							}
						}
					}
				}
			}

			sendMessageToAllNeighbors(toSend);
			aggregator.aggregate(-2);
			setNewVertexValue(vertex.getValue());
		}

		/**
		 * Computes clusters. For the I vertices compute which are their exemplars among those that have decided at
		 * previous step being exemplars. That is the one with the highest similarity
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

	/*
	* Class to encapsulate the values of the vertices. It has:
	* - weights: map containing the similarities with its neighbors
	* - oldValues: 	map with the old sent values for this vertex. Used for damping, with a 0 damping value this will not
	* 				be used
	* - exemplar: variable that contains which exemplar is the one this vertex has selected
	* - convergenceFactorCounter: counter to maintain the counter of the converged times
	*/
	public static final class APVertexValue{

		private HashMap<Long,Double> weights;
		private HashMap<Long,Double> oldValues;
		private long exemplar;
		private int convergenceFactorCounter;

		public APVertexValue(){
			this.weights = new HashMap<>();
			this.oldValues = new HashMap<>();
			this.exemplar = 0L;
			this.setConvergenceFactorCounter(0);
		}

		public HashMap<Long, Double> getWeights() {
			return weights;
		}

		public HashMap<Long, Double> getOldValues() {
			return oldValues;
		}

		public long getExemplar() {
			return exemplar;
		}

		public void setExemplar(long exemplar) {
			this.exemplar = exemplar;
		}

		public int getConvergenceFactorCounter() {
			return convergenceFactorCounter;
		}

		public void setConvergenceFactorCounter(int convergenceFactorCounter) {
			this.convergenceFactorCounter = convergenceFactorCounter;
		}
	}

	/*
	* Class to encapsulate the messages to be sent:
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
