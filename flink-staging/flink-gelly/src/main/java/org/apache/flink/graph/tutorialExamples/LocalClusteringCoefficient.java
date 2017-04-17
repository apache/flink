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

package org.apache.flink.graph.tutorialExamples;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;


/**
 * This example implements a simple Local Clustering Coefficient algorithm for
 * a directed graph.
 *
 * The edges input file is expected to contain one edge per line, with long IDs,
 * in the following format:"<sourceVertexID>\t<targetVertexID>".
 *
 *
 */

public class LocalClusteringCoefficient {


	private static String edgeInputPath = null;
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
		if (args.length == 1) {			
			edgeInputPath = args[0];
		} else {
			System.err.println("Usage: <input edges path> ");
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		//read the Edge DataSet from the input file
		DataSet<Edge<Long, NullValue>> links =  env.readCsvFile(edgeInputPath)
				.fieldDelimiter("\t")
				.lineDelimiter("\n")
				.types(Long.class, Long.class)
				.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {
					public Edge<Long, NullValue> map(Tuple2<Long, Long> value) {
						return new Edge<Long, NullValue>(value.f0, value.f1,
								NullValue.getInstance());
						}
					});

		//create a graph initializing vertex values to be later updated with info 
		//about neighbors.
		Graph<Long, HashMap<String,HashSet<Long>>, NullValue> graph = Graph.fromDataSet(links, new MapFunction<Long, HashMap<String,HashSet<Long>>>() {

			public HashMap<String, HashSet<Long>> map(Long value) throws Exception {
				return  new HashMap<String,HashSet<Long>>();
			}
		}, env);
		
		
        //for each vertex update its value. 
		//Vertex value: Set of incoming and set of outgoing neighbors
        DataSet<Tuple2<Long,HashMap<String,HashSet<Long>>>> neighbSet = graph.groupReduceOnEdges(new NeighborsSet(),EdgeDirection.ALL);

		
		//perform a join to update the vertex values with neighbor's info
        Graph<Long, HashMap<String,HashSet<Long>>, NullValue> graph2 = graph.joinWithVertices(neighbSet,
        		new MapFunction<Tuple2<HashMap<String,HashSet<Long>>,HashMap<String,HashSet<Long>>>,HashMap<String,HashSet<Long>>>(){
					@Override
					public HashMap<String,HashSet<Long>> map(Tuple2<HashMap<String,HashSet<Long>>,HashMap<String,HashSet<Long>>> value) throws Exception {
						return value.f1;
					}});
	
        //calculate the local coeff for each vertex
        DataSet<Tuple2<Long,Double>> coefficients= graph2.groupReduceOnNeighbors(new CoefficientCalc(), EdgeDirection.ALL);
		
        coefficients.print();
        
	}
	
	

	
	@SuppressWarnings("serial")
	static final class NeighborsSet implements EdgesFunction<Long,NullValue,Tuple2<Long,HashMap<String,HashSet<Long>>>> {


		@Override
		public void iterateEdges(
				Iterable<Tuple2<Long, Edge<Long, NullValue>>> edges,
				Collector<Tuple2<Long, HashMap<String,HashSet<Long>>>> out) throws Exception {

			Long vertexId = null;
			HashMap<String,HashSet<Long>> neighbList= new HashMap<String,HashSet<Long>>();
			neighbList.put("incoming", new HashSet<Long>());
			neighbList.put("outgoing", new HashSet<Long>());
			
			
			for(Tuple2<Long, Edge<Long, NullValue>> e: edges){
				
				vertexId= e.f0;
				if(e.f1.getTarget().equals(vertexId))
					neighbList.get("incoming").add(e.f1.getSource());
				else
					neighbList.get("outgoing").add(e.f1.getTarget());
					
				
			}
			out.collect(new Tuple2<Long, HashMap<String,HashSet<Long>>>(vertexId,neighbList));
			
		}

	}
	
	
	
	
	@SuppressWarnings("serial")
	static class CoefficientCalc implements NeighborsFunctionWithVertexValue
	<Long,HashMap<String,HashSet<Long>>,NullValue, Tuple2<Long,Double>> {

		@Override
		public void iterateNeighbors(
				Vertex<Long, HashMap<String,HashSet<Long>>> vertex,
				Iterable<Tuple2<Edge<Long, NullValue>, Vertex<Long, HashMap<String,HashSet<Long>>>>> neighbors,
				Collector<Tuple2<Long, Double>> out) throws Exception {

			double coefficient;
			double numerator = 0;
			
			HashSet<Long> neighbCount= new HashSet<Long>();
			neighbCount.addAll(vertex.f1.get("incoming"));
			neighbCount.addAll(vertex.f1.get("outgoing"));
			
			//Set has been used to remove duplicate neighbors: 
			//those present both in incoming and outgoing are to be counted only once
			int denominator =neighbCount.size()*(neighbCount.size()-1);
			
			//to store links amongst neighbors
			HashSet<String> links = new HashSet<String>();
			
			for(Tuple2<Edge<Long,NullValue>,Vertex<Long,HashMap<String,HashSet<Long>>>> n : neighbors){
				for(Long id : n.f1.getValue().get("incoming")){
					if(vertex.f1.get("incoming").contains(id)||vertex.f1.get("outgoing").contains(id)){
						links.add(""+id+" "+n.f1.f0);
					}
				}
				for(Long id : n.f1.getValue().get("outgoing")){
					if(vertex.f1.get("incoming").contains(id)||vertex.f1.get("outgoing").contains(id)){
						links.add(""+n.f1.f0+" "+id);
					}
				}
			}
			
			//number of connections amongst neighbors
			numerator=links.size();
			
			//for debug
			//System.out.println("vertex " +vertex.f0+ " links: "+links.toString());
			//System.out.println("count " +count+"  "+den);
			
			if(denominator<1)
				coefficient=0.0;
			else
				coefficient=(double)numerator/(double)denominator;
			
			out.collect(new Tuple2<Long,Double>(vertex.f0,coefficient));
			
		}}
	
	
}
