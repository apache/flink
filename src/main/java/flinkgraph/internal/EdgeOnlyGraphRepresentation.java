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

package flinkgraph.internal;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

import flinkgraph.api.Edge;
import flinkgraph.internal.utils.Tuple1Unpackfunction;

@SuppressWarnings("serial")
public class EdgeOnlyGraphRepresentation<T> extends AbstractGraphRepresentation<T> {
	
	
	private final DataSet<? extends Edge<T>> edges;
	
	
	public EdgeOnlyGraphRepresentation(DataSet<? extends Edge<T>> edges) {
		this.edges = edges;
	}
	
	
	
	@Override
	public DataSet<? extends Edge<T>> getEdges() {
		return edges;
	}
	
	@Override
	public DataSet<T> getVertexIdSet() {
		DataSet<Tuple1<T>> wrapped = edges.flatMap(new ToIdMapper<T>()).distinct();
		// currently need to go through the tuple wrapped variant because distinct
		// is only available on tuples
		return wrapped.map(new Tuple1Unpackfunction<T>());
	}
	
	
	// --------------------------------------------------------------------------------------------
	
	public static final class ToIdMapper<T> implements FlatMapFunction<Edge<T>, Tuple1<T>> {
		
		@Override
		public void flatMap(Edge<T> value, Collector<Tuple1<T>> out) {
			// collect both source and target, because we cannot be sure 
			out.collect(new Tuple1<T>(value.f0));
			out.collect(new Tuple1<T>(value.f1));
		}
	}
}
