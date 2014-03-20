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
package eu.stratosphere.example.java.triangles.util;

import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;

public class EdgeDataTypes {

	public static class Edge extends Tuple2<Integer, Integer> {
		private static final long serialVersionUID = 1L;
		
		public static final int V1 = 0;
		public static final int V2 = 1;
		
		public Edge() {}
		
		public Edge(final Integer v1, final Integer v2) {
			this.setFirstVertex(v1);
			this.setSecondVertex(v2);
		}
		
		public Integer getFirstVertex() { return this.getField(V1); }
		
		public Integer getSecondVertex() { return this.getField(V2); }
		
		public void setFirstVertex(final Integer vertex1) { this.setField(vertex1, V1); }
		
		public void setSecondVertex(final Integer vertex2) { this.setField(vertex2, V2); }
		
		public void copyVerticesFromTuple2(Tuple2<Integer, Integer> t) {
			this.setFirstVertex(t.f0);
			this.setSecondVertex(t.f1);
		}
		
		public void copyVerticesFromEdgeWithDegrees(EdgeWithDegrees ewd) {
			this.setFirstVertex(ewd.getFirstVertex());
			this.setSecondVertex(ewd.getSecondVertex());
		}
		
		public void flipVertics() {
			Integer tmp = this.getFirstVertex();
			this.setFirstVertex(this.getSecondVertex());
			this.setSecondVertex(tmp);
		}
	}
	
	public static class Triad extends Tuple3<Integer, Integer, Integer> {
		private static final long serialVersionUID = 1L;
		
		public static final int V1 = 0;
		public static final int V2 = 1;
		public static final int V3 = 2;
		
		public Triad() {}
		
		public void setFirstVertex(final Integer vertex1) { this.setField(vertex1, V1); }
		
		public void setSecondVertex(final Integer vertex2) { this.setField(vertex2, V2); }
		
		public void setThirdVertex(final Integer vertex3) { this.setField(vertex3, V3); }
	}
	
	public static class EdgeWithDegrees extends Tuple4<Integer, Integer, Integer, Integer> {
		private static final long serialVersionUID = 1L;

		public static final int V1 = 0;
		public static final int V2 = 1;
		public static final int D1 = 2;
		public static final int D2 = 3;
		
		public EdgeWithDegrees() { }
		
		public EdgeWithDegrees(final Integer vertex1, final Integer degree1, final Integer vertex2, final Integer degree2) {
			super(vertex1, degree1, vertex2, degree2);
		}
		
		public Integer getFirstVertex() { return this.getField(V1); }
		
		public Integer getSecondVertex() { return this.getField(V2); }
		
		public Integer getFirstDegree() { return this.getField(D1); }
		
		public Integer getSecondDegree() { return this.getField(D2); }
		
		public void setFirstVertex(final Integer vertex1) { this.setField(vertex1, V1); }
		
		public void setSecondVertex(final Integer vertex2) { this.setField(vertex2, V2); }
		
		public void setFirstDegree(final Integer degree1) { this.setField(degree1, D1); }
		
		public void setSecondDegree(final Integer degree2) { this.setField(degree2, D2); }
		
		public void copyFrom(final EdgeWithDegrees edge) {
			this.setFirstVertex(edge.getFirstVertex());
			this.setSecondVertex(edge.getSecondVertex());
			this.setFirstDegree(edge.getFirstDegree());
			this.setSecondDegree(edge.getSecondDegree());
		}
	}
	
	
	public static class TupleEdgeConverter extends MapFunction<Tuple2<Integer, Integer>, Edge> {
		private static final long serialVersionUID = 1L;

		private final Edge outEdge = new Edge();
		
		@Override
		public Edge map(Tuple2<Integer, Integer> t) throws Exception {
			outEdge.copyVerticesFromTuple2(t);
			return outEdge;
		}
		
	}
	
}
