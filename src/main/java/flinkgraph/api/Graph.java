/**
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

package flinkgraph.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import flinkgraph.api.io.EdgeBuilder;
import flinkgraph.internal.AbstractGraphRepresentation;
import flinkgraph.internal.EdgeOnlyGraphRepresentation;


public class Graph<T extends Comparable<T>> {
	
	
	private AbstractGraphRepresentation<T> graphRepr;
	
	private long vertexCount = -1;
	
	private long edgeCount = -1;

	// --------------------------------------------------------------------------------------------
	
	public Graph(AbstractGraphRepresentation<T> representation) {
		this.graphRepr = representation;
	}
	
	// --------------------------------------------------------------------------------------------

	
	public long getNumberOfVertices() {
		if (vertexCount == -1) {
			vertexCount = count(graphRepr.getVertexIdSet());
		}
		return vertexCount;
	}
	
	public long getNumberOfEdges() {
		if (edgeCount == -1) {
			edgeCount = count(graphRepr.getEdges());
		}
		return edgeCount;
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Utility Functions
	// --------------------------------------------------------------------------------------------
	
	public static <E> int count(DataSet<E> data) {
		return -1; // to be implemented
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Static Builders
	// --------------------------------------------------------------------------------------------

	public static <T extends Comparable<T>> Graph<T> fromEdges(ExecutionEnvironment e, Class<T> idType, EdgeBuilder<T> edgeBuilder) {
		DataSet<? extends Edge<T>> edges = edgeBuilder.createEdges(e, idType);
		return new Graph<T>(new EdgeOnlyGraphRepresentation<T>(edges));
	}

}
