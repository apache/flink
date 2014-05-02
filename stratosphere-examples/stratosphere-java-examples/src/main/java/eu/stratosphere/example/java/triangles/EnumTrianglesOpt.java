/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.example.java.triangles;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.example.java.triangles.util.EdgeData;
import eu.stratosphere.example.java.triangles.util.EdgeDataTypes.Edge;
import eu.stratosphere.example.java.triangles.util.EdgeDataTypes.EdgeWithDegrees;
import eu.stratosphere.example.java.triangles.util.EdgeDataTypes.Triad;
import eu.stratosphere.example.java.triangles.util.EdgeDataTypes.TupleEdgeConverter;
import eu.stratosphere.util.Collector;

/**
 * Triangle enumeration is a preprocessing step to find closely connected parts in graphs.
 * A triangle are three edges that connect three vertices with each other.
 * 
 * The basic algorithm works as follows: 
 * It groups all edges that share a common vertex and builds triads, i.e., triples of vertices 
 * that are connected by two edges. Finally, all triads are filtered for which no third edge exists 
 * that closes the triangle.
 * 
 * For a group of n edges that share a common vertex, the number of built triads is quadratic ((n*(n-1))/2).
 * Therefore, an optimization of the algorithm is to group edges on the vertex with the smaller output degree to 
 * reduce the number of triads. 
 * This implementation extends the basic algorithm by computing output degrees of edge vertices and 
 * grouping on edges on the vertex with the smaller degree.
 *  
 * This implementation assumes that edges are represented as pairs of vertices and 
 * vertices are represented as Integer IDs.
 * 
 * The lines of input files need to have the following format:
 * "<INT: vertexId1>,<INT: vertexId2>\n"
 * 
 * For example the input: 
 * "10,20\n10,30\n20,30\n" 
 * defines three edges (10,20), (10,30), (20,30) which build a triangle.
 * 
 */
public class EnumTrianglesOpt {

	/**
	 * Emits for an edge the original edge and its switched version.
	 */
	private static class EdgeDuplicator extends FlatMapFunction<Edge, Edge> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void flatMap(Edge edge, Collector<Edge> out) throws Exception {
			out.collect(edge);
			edge.flipVertics();
			out.collect(edge);
		}
	}
	
	/**
	 * Counts the number of edges that share a common vertex.
	 * Emits one edge for each input edge with a degree annotation for the shared vertex.
	 * For each emitted edge, the first vertex is the vertex with the smaller id.
	 */
	private static class DegreeCounter extends GroupReduceFunction<Edge, EdgeWithDegrees> {
		private static final long serialVersionUID = 1L;
		
		final ArrayList<Integer> otherVertices = new ArrayList<Integer>();
		final EdgeWithDegrees outputEdge = new EdgeWithDegrees();
		
		@Override
		public void reduce(Iterator<Edge> edges, Collector<EdgeWithDegrees> out) throws Exception {
			
			otherVertices.clear();
			
			// get first edge
			Edge edge = edges.next();
			Integer groupVertex = edge.getFirstVertex();
			this.otherVertices.add(edge.getSecondVertex());
			
			// get all other edges (assumes edges are sorted by second vertex)
			while(edges.hasNext()) {
				edge = edges.next();
				Integer otherVertex = edge.getSecondVertex();
				// collect unique vertices
				if(!otherVertices.contains(otherVertex) && otherVertex != groupVertex) {
					this.otherVertices.add(otherVertex);
				}
			}
			int degree = this.otherVertices.size();
			
			// emit edges
			for(Integer otherVertex : this.otherVertices) {
				if(groupVertex < otherVertex) {
					outputEdge.setFirstVertex(groupVertex);
					outputEdge.setFirstDegree(degree);
					outputEdge.setSecondVertex(otherVertex);
					outputEdge.setSecondDegree(0);
				} else {
					outputEdge.setFirstVertex(otherVertex);
					outputEdge.setFirstDegree(0);
					outputEdge.setSecondVertex(groupVertex);
					outputEdge.setSecondDegree(degree);
				}
				out.collect(outputEdge);
			}
		}
	}
	
	/**
	 * Builds an edge with degree annotation from two edges that have the same vertices and only one 
	 * degree annotation.
	 */
	private static class DegreeJoiner extends ReduceFunction<EdgeWithDegrees> {
		private static final long serialVersionUID = 1L;
		private final EdgeWithDegrees outEdge = new EdgeWithDegrees();
		
		@Override
		public EdgeWithDegrees reduce(EdgeWithDegrees edge1, EdgeWithDegrees edge2) throws Exception {
			
			// copy first edge
			outEdge.copyFrom(edge1);
			
			// set missing degree
			if(edge1.getFirstDegree() == 0 && edge1.getSecondDegree() != 0) {
				outEdge.setFirstDegree(edge2.getFirstDegree());
			} else if (edge1.getFirstDegree() != 0 && edge1.getSecondDegree() == 0) {
				outEdge.setSecondDegree(edge2.getSecondDegree());
			}
			return outEdge;
		}
	}
		
	/**
	 *  Projects an edge (pair of vertices) such that the first vertex is the vertex with the smaller degree.
	 */
	private static class EdgeByDegreeProjector extends MapFunction<EdgeWithDegrees, Edge> {
		private static final long serialVersionUID = 1L;
		
		private final Edge outEdge = new Edge();
		
		@Override
		public Edge map(EdgeWithDegrees inEdge) throws Exception {

			// copy vertices to simple edge
			outEdge.copyVerticesFromEdgeWithDegrees(inEdge);

			// flip vertices if first degree is larger than second degree.
			if(inEdge.getFirstDegree() > inEdge.getSecondDegree()) {
				outEdge.flipVertics();
			}

			// return edge
			return outEdge;
		}
	}
	
	/**
	 *  Projects an edge (pair of vertices) such that the id of the first is smaller than the id of the second. 
	 */
	private static class EdgeByIdProjector extends MapFunction<Edge, Edge> {
		private static final long serialVersionUID = 1L;
	
		@Override
		public Edge map(Edge inEdge) throws Exception {
			
			// flip vertices if necessary
			if(inEdge.getFirstVertex() > inEdge.getSecondVertex()) {
				inEdge.flipVertics();
			}
			
			return inEdge;
		}
	}
	
	/**
	 *  Builds triads (triples of vertices) from pairs of edges that share a vertex.
	 *  The first vertex of a triad is the shared vertex, the second and third vertex are ordered by vertexId. 
	 *  Assumes that input edges share the first vertex and are in ascending order of the second vertex.
	 */
	private static class TriadBuilder extends GroupReduceFunction<Edge, Triad> {
		private static final long serialVersionUID = 1L;
		
		private final List<Integer> vertices = new ArrayList<Integer>();
		private final Triad outTriad = new Triad();
		
		@Override
		public void reduce(Iterator<Edge> edges, Collector<Triad> out) throws Exception {
			
			// clear vertex list
			vertices.clear();
			
			// read first edge
			Edge firstEdge = edges.next();
			outTriad.setFirstVertex(firstEdge.getFirstVertex());
			vertices.add(firstEdge.getSecondVertex());
			
			// build and emit triads
			while(edges.hasNext()) {
				Integer higherVertexId = edges.next().getSecondVertex();
				
				// combine vertex with all previously read vertices
				for(Integer lowerVertexId : vertices) {
					outTriad.setSecondVertex(lowerVertexId);
					outTriad.setThirdVertex(higherVertexId);
					out.collect(outTriad);
				}
				vertices.add(higherVertexId);
			}
		}
	}
	
	/**
	 *  Filters triads (three vertices connected by two edges) without a closing third edge.
	 */
	private static class TriadFilter extends JoinFunction<Triad, Edge, Triad> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public Triad join(Triad triad, Edge edge) throws Exception {
			return triad;
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		String edgePath = "TESTDATA";
		String outPath = "STDOUT";
		
		// parse input arguments
		if(args.length > 0) {
			edgePath = args[0];
		}
		if(args.length > 1) {
			outPath = args[1];
		}
		
		// set up execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	
		// read input data
		DataSet<Edge> edges;
		if(edgePath.equals("TESTDATA")) {
			edges = EdgeData.getDefaultEdgeDataSet(env);
		} else {
			edges = env.readCsvFile(edgePath)
				.fieldDelimiter(',')
				.includeFields(true, true)
				.types(Integer.class, Integer.class)
				.map(new TupleEdgeConverter());
		}
		
		// annotate edges with degrees
		DataSet<EdgeWithDegrees> edgesWithDegrees = edges
				.flatMap(new EdgeDuplicator())
				.groupBy(Edge.V1).sortGroup(Edge.V2, Order.ASCENDING).reduceGroup(new DegreeCounter())
				.groupBy(EdgeWithDegrees.V1,EdgeWithDegrees.V2).reduce(new DegreeJoiner());
		
		// project edges by degrees
		DataSet<Edge> edgesByDegree = edgesWithDegrees
				.map(new EdgeByDegreeProjector());
		// project edges by vertex id
		DataSet<Edge> edgesById = edgesByDegree
				.map(new EdgeByIdProjector());
		
		// build and filter triads
		DataSet<Triad> triangles = edgesByDegree
				.groupBy(Edge.V1).sortGroup(Edge.V2, Order.ASCENDING).reduceGroup(new TriadBuilder())
				.join(edgesById).where(Triad.V2,Triad.V3).equalTo(Edge.V1,Edge.V2).with(new TriadFilter());

		// emit triangles
		if(outPath.equals("STDOUT")) {
			triangles.print();
		} else {
			triangles.writeAsCsv(outPath, "\n", ",");
		}
		
		// execute program		
		env.execute();
	}
}
