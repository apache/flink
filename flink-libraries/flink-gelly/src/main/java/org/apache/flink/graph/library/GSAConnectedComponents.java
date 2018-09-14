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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.utils.GraphUtils.MapTo;
import org.apache.flink.types.NullValue;

/**
 * A gather-sum-apply implementation of the Weakly Connected Components algorithm.
 *
 * <p>This implementation uses a comparable vertex value as initial component
 * identifier (ID). In the gather phase, each vertex collects the vertex value
 * of their adjacent vertices. In the sum phase, the minimum among those values
 * is selected. In the apply phase, the algorithm sets the minimum value as the
 * new vertex value if it is smaller than the current value.
 *
 * <p>The algorithm converges when vertices no longer update their component ID
 * value or when the maximum number of iterations has been reached.
 *
 * <p>The result is a DataSet of vertices, where the vertex value corresponds to
 * the assigned component ID.
 *
 * @see ConnectedComponents
 */
public class GSAConnectedComponents<K, VV extends Comparable<VV>, EV>
	implements GraphAlgorithm<K, VV, EV, DataSet<Vertex<K, VV>>> {

	private Integer maxIterations;

	/**
	 * Creates an instance of the GSA Connected Components algorithm.
	 * The algorithm computes weakly connected components
	 * and converges when no vertex updates its component ID
	 * or when the maximum number of iterations has been reached.
	 *
	 * @param maxIterations The maximum number of iterations to run.
	 */
	public GSAConnectedComponents(Integer maxIterations) {
		this.maxIterations = maxIterations;
	}

	@Override
	public DataSet<Vertex<K, VV>> run(Graph<K, VV, EV> graph) throws Exception {

		// get type information for vertex value
		TypeInformation<VV> valueTypeInfo = ((TupleTypeInfo<?>) graph.getVertices().getType()).getTypeAt(1);

		Graph<K, VV, NullValue> undirectedGraph = graph
			.mapEdges(new MapTo<>(NullValue.getInstance()))
			.getUndirected();

		return undirectedGraph.runGatherSumApplyIteration(
			new GatherNeighborIds<>(valueTypeInfo),
			new SelectMinId<>(valueTypeInfo),
			new UpdateComponentId<>(valueTypeInfo),
			maxIterations).getVertices();
	}

	// --------------------------------------------------------------------------------------------
	//  Connected Components UDFs
	// --------------------------------------------------------------------------------------------

	private static final class GatherNeighborIds<VV extends Comparable<VV>>
		extends GatherFunction<VV, NullValue, VV>
		implements ResultTypeQueryable<VV> {

		private final TypeInformation<VV> typeInformation;

		private GatherNeighborIds(TypeInformation<VV> typeInformation) {
			this.typeInformation = typeInformation;
		}

		public VV gather(Neighbor<VV, NullValue> neighbor) {
			return neighbor.getNeighborValue();
		}

		@Override
		public TypeInformation<VV> getProducedType() {
			return typeInformation;
		}
	}

	private static final class SelectMinId<VV extends Comparable<VV>>
		extends SumFunction<VV, NullValue, VV>
		implements ResultTypeQueryable<VV> {

		private final TypeInformation<VV> typeInformation;

		private SelectMinId(TypeInformation<VV> typeInformation) {
			this.typeInformation = typeInformation;
		}

		public VV sum(VV newValue, VV currentValue) {
			return newValue.compareTo(currentValue) < 0 ? newValue : currentValue;
		}

		@Override
		public TypeInformation<VV> getProducedType() {
			return typeInformation;
		}
	}

	private static final class UpdateComponentId<K, VV extends Comparable<VV>>
		extends ApplyFunction<K, VV, VV>
		implements ResultTypeQueryable<VV> {

		private final TypeInformation<VV> typeInformation;

		private UpdateComponentId(TypeInformation<VV> typeInformation) {
			this.typeInformation = typeInformation;
		}

		public void apply(VV summedValue, VV origValue) {
			if (summedValue.compareTo(origValue) < 0) {
				setResult(summedValue);
			}
		}

		@Override
		public TypeInformation<VV> getProducedType() {
			return typeInformation;
		}
	}
}
