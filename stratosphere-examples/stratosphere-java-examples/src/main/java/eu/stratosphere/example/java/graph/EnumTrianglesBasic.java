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
package eu.stratosphere.example.java.graph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.example.java.graph.util.EnumTrianglesData;
import eu.stratosphere.example.java.graph.util.EnumTrianglesDataTypes.Edge;
import eu.stratosphere.example.java.graph.util.EnumTrianglesDataTypes.Triad;
import eu.stratosphere.util.Collector;

/**
 * Triangle enumeration is a preprocessing step to find closely connected parts in graphs.
 * A triangle consists of three edges that connect three vertices with each other.
 * 
 * <p>
 * The algorithm works as follows: 
 * It groups all edges that share a common vertex and builds triads, i.e., triples of vertices 
 * that are connected by two edges. Finally, all triads are filtered for which no third edge exists 
 * that closes the triangle.
 *  
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Edges are represented as pairs for vertex IDs which are separated by space 
 * characters. Edges are separated by new-line characters.<br>
 * For example <code>"1 2\n2 12\n1 12\n42 63\n"</code> gives four (undirected) edges (1)-(2), (2)-(12), (1)-(12), and (42)-(63)
 * that include a triangle
 * </ul>
 * <pre>
 *     (1)
 *     /  \
 *   (2)-(12)
 * </pre>
 * 
 * Usage: <code>EnumTriangleBasic &lt;edge path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link EnumTrianglesData}. 
 * 
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>Custom Java objects which extend Tuple
 * <li>Group Sorting
 * </ul>
 * 
 */
@SuppressWarnings("serial")
public class EnumTrianglesBasic {

	static boolean fileOutput = false;
	static String edgePath = null;
	static String outputPath = null;
	
	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	
	public static void main(String[] args) throws Exception {
		
		parseParameters(args);
		
		// set up execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	
		// read input data
		DataSet<Edge> edges = getEdgeDataSet(env);
		
		// project edges by vertex id
		DataSet<Edge> edgesById = edges
				.map(new EdgeByIdProjector());
		
		DataSet<Triad> triangles = edgesById
				// build triads
				.groupBy(Edge.V1).sortGroup(Edge.V2, Order.ASCENDING).reduceGroup(new TriadBuilder())
				// filter triads
				.join(edgesById).where(Triad.V2, Triad.V3).equalTo(Edge.V1, Edge.V2).with(new TriadFilter());

		// emit result
		if(fileOutput) {
			triangles.writeAsCsv(outputPath, "\n", ",");
		} else {
			triangles.print();
		}

		// execute program
		env.execute("Basic Triangle Enumeration Example");

	}
	
	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/** Converts a Tuple2 into an Edge */
	public static class TupleEdgeConverter extends MapFunction<Tuple2<Integer, Integer>, Edge> {
		private final Edge outEdge = new Edge();
		
		@Override
		public Edge map(Tuple2<Integer, Integer> t) throws Exception {
			outEdge.copyVerticesFromTuple2(t);
			return outEdge;
		}
	}
	
	/** Projects an edge (pair of vertices) such that the id of the first is smaller than the id of the second. */
	private static class EdgeByIdProjector extends MapFunction<Edge, Edge> {
	
		@Override
		public Edge map(Edge inEdge) throws Exception {
			
			// flip vertices if necessary
			if(inEdge.getFirstVertex() > inEdge.getSecondVertex()) {
				inEdge.flipVertices();
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
	
	/** Filters triads (three vertices connected by two edges) without a closing third edge. */
	private static class TriadFilter extends JoinFunction<Triad, Edge, Triad> {
		
		@Override
		public Triad join(Triad triad, Edge edge) throws Exception {
			return triad;
		}
	}
	
	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************
	
	private static void parseParameters(String[] args) {
	
		if(args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if(args.length == 2) {
				edgePath = args[0];
				outputPath = args[1];
			} else {
				System.err.println("Usage: EnumTriangleBasic <edge path> <result path>");
				System.exit(1);
			}
		} else {
			System.out.println("Executing Enum Triangles Basic example with built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: EnumTriangleBasic <edge path> <result path>");
		}
	}
	
	private static DataSet<Edge> getEdgeDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
			return env.readCsvFile(edgePath)
						.fieldDelimiter(' ')
						.includeFields(true, true)
						.types(Integer.class, Integer.class)
						.map(new TupleEdgeConverter());
		} else {
			return EnumTrianglesData.getDefaultEdgeDataSet(env);
		}
	}
	
}
