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

package org.apache.flink.graph.gsa;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

import com.google.common.base.Preconditions;

/**
 * This class represents iterative graph computations, programmed in a gather-sum-apply perspective.
 *
 * @param <K> The type of the vertex key in the graph
 * @param <VV> The type of the vertex value in the graph
 * @param <EV> The type of the edge value in the graph
 * @param <M> The intermediate type used by the gather, sum and apply functions
 */
public class GatherSumApplyIteration<K, VV, EV, M> implements CustomUnaryOperation<Vertex<K, VV>,
		Vertex<K, VV>> {

	private DataSet<Vertex<K, VV>> vertexDataSet;
	private DataSet<Edge<K, EV>> edgeDataSet;

	private final GatherFunction<VV, EV, M> gather;
	private final SumFunction<VV, EV, M> sum;
	private final ApplyFunction<K, VV, M> apply;
	private final int maximumNumberOfIterations;

	// ----------------------------------------------------------------------------------

	private GatherSumApplyIteration(GatherFunction<VV, EV, M> gather, SumFunction<VV, EV, M> sum,
			ApplyFunction<K, VV, M> apply, DataSet<Edge<K, EV>> edges, int maximumNumberOfIterations) {

		Preconditions.checkNotNull(gather);
		Preconditions.checkNotNull(sum);
		Preconditions.checkNotNull(apply);
		Preconditions.checkNotNull(edges);
		Preconditions.checkArgument(maximumNumberOfIterations > 0, "The maximum number of iterations must be at least one.");

		this.gather = gather;
		this.sum = sum;
		this.apply = apply;
		this.edgeDataSet = edges;
		this.maximumNumberOfIterations = maximumNumberOfIterations;
	}

	// --------------------------------------------------------------------------------------------
	//  Custom Operator behavior
	// --------------------------------------------------------------------------------------------

	/**
	 * Sets the input data set for this operator. In the case of this operator this input data set represents
	 * the set of vertices with their initial state.
	 *
	 * @param dataSet The input data set, which in the case of this operator represents the set of
	 *                vertices with their initial state.
	 */
	@Override
	public void setInput(DataSet<Vertex<K, VV>> dataSet) {
		this.vertexDataSet = dataSet;
	}

	/**
	 * Computes the results of the gather-sum-apply iteration
	 *
	 * @return The resulting DataSet
	 */
	@Override
	public DataSet<Vertex<K, VV>> createResult() {
		if (vertexDataSet == null) {
			throw new IllegalStateException("The input data set has not been set.");
		}

		// Prepare type information
		TypeInformation<K> keyType = ((TupleTypeInfo<?>) vertexDataSet.getType()).getTypeAt(0);
		TypeInformation<M> messageType = TypeExtractor.createTypeInfo(GatherFunction.class, gather.getClass(), 2, null, null);
		TypeInformation<Tuple2<K, M>> innerType = new TupleTypeInfo<Tuple2<K, M>>(keyType, messageType);
		TypeInformation<Vertex<K, VV>> outputType = vertexDataSet.getType();

		// Prepare UDFs
		GatherUdf<K, VV, EV, M> gatherUdf = new GatherUdf<K, VV, EV, M>(gather, innerType);
		SumUdf<K, VV, EV, M> sumUdf = new SumUdf<K, VV, EV, M>(sum, innerType);
		ApplyUdf<K, VV, EV, M> applyUdf = new ApplyUdf<K, VV, EV, M>(apply, outputType);

		final int[] zeroKeyPos = new int[] {0};
		final DeltaIteration<Vertex<K, VV>, Vertex<K, VV>> iteration =
				vertexDataSet.iterateDelta(vertexDataSet, maximumNumberOfIterations, zeroKeyPos);

		// Prepare the neighbors
		DataSet<Tuple2<K, Neighbor<VV, EV>>> neighbors = iteration
				.getWorkset().join(edgeDataSet)
				.where(0).equalTo(0).with(new ProjectKeyWithNeighbor<K, VV, EV>());

		// Gather, sum and apply
		DataSet<Tuple2<K, M>> gatheredSet = neighbors.map(gatherUdf);
		DataSet<Tuple2<K, M>> summedSet = gatheredSet.groupBy(0).reduce(sumUdf);
		JoinOperator<?, ?, Vertex<K, VV>> appliedSet = summedSet
				.join(iteration.getSolutionSet())
				.where(0)
				.equalTo(0)
				.with(applyUdf);

		// let the operator know that we preserve the key field
		appliedSet.withForwardedFieldsFirst("0").withForwardedFieldsSecond("0");

		return iteration.closeWith(appliedSet, appliedSet);
	}

	/**
	 * Creates a new gather-sum-apply iteration operator for graphs
	 *
	 * @param edges The edge DataSet
	 *
	 * @param gather The gather function of the GSA iteration
	 * @param sum The sum function of the GSA iteration
	 * @param apply The apply function of the GSA iteration
	 *
	 * @param maximumNumberOfIterations The maximum number of iterations executed
	 *
	 * @param <K> The type of the vertex key in the graph
	 * @param <VV> The type of the vertex value in the graph
	 * @param <EV> The type of the edge value in the graph
	 * @param <M> The intermediate type used by the gather, sum and apply functions
	 *
	 * @return An in stance of the gather-sum-apply graph computation operator.
	 */
	public static final <K, VV, EV, M> GatherSumApplyIteration<K, VV, EV, M>
		withEdges(DataSet<Edge<K, EV>> edges, GatherFunction<VV, EV, M> gather,
		SumFunction<VV, EV, M> sum, ApplyFunction<K, VV, M> apply, int maximumNumberOfIterations) {

		return new GatherSumApplyIteration<K, VV, EV, M>(gather, sum, apply, edges, maximumNumberOfIterations);
	}

	// --------------------------------------------------------------------------------------------
	//  Wrapping UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	@ForwardedFields("f0")
	private static final class GatherUdf<K, VV, EV, M> extends RichMapFunction<Tuple2<K, Neighbor<VV, EV>>,
			Tuple2<K, M>> implements ResultTypeQueryable<Tuple2<K, M>> {

		private final GatherFunction<VV, EV, M> gatherFunction;
		private transient TypeInformation<Tuple2<K, M>> resultType;

		private GatherUdf(GatherFunction<VV, EV, M> gatherFunction, TypeInformation<Tuple2<K, M>> resultType) {
			this.gatherFunction = gatherFunction;
			this.resultType = resultType;
		}

		@Override
		public Tuple2<K, M> map(Tuple2<K, Neighbor<VV, EV>> neighborTuple) {
			M result = this.gatherFunction.gather(neighborTuple.f1);
			return new Tuple2<K, M>(neighborTuple.f0, result);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
				this.gatherFunction.init(getIterationRuntimeContext());
			}
			this.gatherFunction.preSuperstep();
		}

		@Override
		public void close() throws Exception {
			this.gatherFunction.postSuperstep();
		}

		@Override
		public TypeInformation<Tuple2<K, M>> getProducedType() {
			return this.resultType;
		}
	}

	@SuppressWarnings("serial")
	private static final class SumUdf<K, VV, EV, M> extends RichReduceFunction<Tuple2<K, M>>
			implements ResultTypeQueryable<Tuple2<K, M>>{

		private final SumFunction<VV, EV, M> sumFunction;
		private transient TypeInformation<Tuple2<K, M>> resultType;

		private SumUdf(SumFunction<VV, EV, M> sumFunction, TypeInformation<Tuple2<K, M>> resultType) {
			this.sumFunction = sumFunction;
			this.resultType = resultType;
		}

		@Override
		public Tuple2<K, M> reduce(Tuple2<K, M> arg0, Tuple2<K, M> arg1) throws Exception {
			K key = arg0.f0;
			M result = this.sumFunction.sum(arg0.f1, arg1.f1);
			return new Tuple2<K, M>(key, result);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
				this.sumFunction.init(getIterationRuntimeContext());
			}
			this.sumFunction.preSuperstep();
		}

		@Override
		public void close() throws Exception {
			this.sumFunction.postSuperstep();
		}

		@Override
		public TypeInformation<Tuple2<K, M>> getProducedType() {
			return this.resultType;
		}
	}

	@SuppressWarnings("serial")
	private static final class ApplyUdf<K, VV, EV, M> extends RichFlatJoinFunction<Tuple2<K, M>,
			Vertex<K, VV>, Vertex<K, VV>> implements ResultTypeQueryable<Vertex<K, VV>> {

		private final ApplyFunction<K, VV, M> applyFunction;
		private transient TypeInformation<Vertex<K, VV>> resultType;

		private ApplyUdf(ApplyFunction<K, VV, M> applyFunction, TypeInformation<Vertex<K, VV>> resultType) {
			this.applyFunction = applyFunction;
			this.resultType = resultType;
		}

		@Override
		public void join(Tuple2<K, M> newValue, final Vertex<K, VV> currentValue, final Collector<Vertex<K, VV>> out) throws Exception {

			this.applyFunction.setOutput(currentValue, out);
			this.applyFunction.apply(newValue.f1, currentValue.getValue());
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
				this.applyFunction.init(getIterationRuntimeContext());
			}
			this.applyFunction.preSuperstep();
		}

		@Override
		public void close() throws Exception {
			this.applyFunction.postSuperstep();
		}

		@Override
		public TypeInformation<Vertex<K, VV>> getProducedType() {
			return this.resultType;
		}
	}

	@SuppressWarnings("serial")
	@ForwardedFieldsSecond("f1->f0")
	private static final class ProjectKeyWithNeighbor<K, VV, EV> implements FlatJoinFunction<
			Vertex<K, VV>, Edge<K, EV>, Tuple2<K, Neighbor<VV, EV>>> {

		public void join(Vertex<K, VV> vertex, Edge<K, EV> edge, Collector<Tuple2<K, Neighbor<VV, EV>>> out) {
			out.collect(new Tuple2<K, Neighbor<VV, EV>>(
					edge.getTarget(), new Neighbor<VV, EV>(vertex.getValue(), edge.getValue())));
		}
	}

}
