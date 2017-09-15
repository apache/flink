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

package org.apache.flink.graph.library.similarity;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.undirected.VertexDegree;
import org.apache.flink.graph.asm.result.BinaryResult.MirrorResult;
import org.apache.flink.graph.asm.result.BinaryResultBase;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.library.similarity.AdamicAdar.Result;
import org.apache.flink.graph.utils.MurmurHash;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * http://social.cs.uiuc.edu/class/cs591kgk/friendsadamic.pdf
 *
 * <p>Adamic-Adar measures the similarity between pairs of vertices as the sum of
 * the inverse logarithm of degree over shared neighbors. Scores are non-negative
 * and unbounded. A vertex with higher degree has greater overall influence but
 * is less influential to each pair of neighbors.
 *
 * <p>This implementation produces similarity scores for each pair of vertices
 * in the graph with at least one shared neighbor; equivalently, this is the
 * set of all non-zero Adamic-Adar coefficients.
 *
 * <p>The input graph must be a simple, undirected graph containing no duplicate
 * edges or self-loops.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class AdamicAdar<K extends CopyableValue<K>, VV, EV>
extends GraphAlgorithmWrappingDataSet<K, VV, EV, Result<K>> {

	private static final int GROUP_SIZE = 64;

	private static final String SUM_OF_SCORES_AND_NUMBER_OF_NEIGHBOR_PAIRS = "sum of scores and number of vertices";

	// Optional configuration
	private float minimumScore = 0.0f;

	private float minimumRatio = 0.0f;

	private boolean mirrorResults;

	/**
	 * Filter out Adamic-Adar scores less than the given minimum.
	 *
	 * @param score minimum score
	 * @return this
	 */
	public AdamicAdar<K, VV, EV> setMinimumScore(float score) {
		Preconditions.checkArgument(score >= 0, "Minimum score must be non-negative");

		this.minimumScore = score;

		return this;
	}

	/**
	 * Filter out Adamic-Adar scores less than the given ratio times the average score.
	 *
	 * @param ratio minimum ratio
	 * @return this
	 */
	public AdamicAdar<K, VV, EV> setMinimumRatio(float ratio) {
		Preconditions.checkArgument(ratio >= 0, "Minimum ratio must be non-negative");

		this.minimumRatio = ratio;

		return this;
	}

	/**
	 * By default only one result is output for each pair of vertices. When
	 * mirroring a second result with the vertex order flipped is output for
	 * each pair of vertices.
	 *
	 * @param mirrorResults whether output results should be mirrored
	 * @return this
	 */
	public AdamicAdar<K, VV, EV> setMirrorResults(boolean mirrorResults) {
		this.mirrorResults = mirrorResults;

		return this;
	}

	@Override
	protected boolean canMergeConfigurationWith(GraphAlgorithmWrappingBase other) {
		if (!super.canMergeConfigurationWith(other)) {
			return false;
		}

		AdamicAdar rhs = (AdamicAdar) other;

		return minimumRatio == rhs.minimumRatio && minimumScore == rhs.minimumScore;
	}

	/*
	 * Implementation notes:
	 *
	 * The requirement that "K extends CopyableValue<K>" can be removed when
	 *   Flink has a self-join which performs the skew distribution handled by
	 *   GenerateGroupSpans / GenerateGroups / GenerateGroupPairs.
	 */

	@Override
	public DataSet<Result<K>> runInternal(Graph<K, VV, EV> input)
			throws Exception {
		// s, d(s), 1/log(d(s))
		DataSet<Tuple3<K, LongValue, FloatValue>> inverseLogDegree = input
			.run(new VertexDegree<K, VV, EV>()
				.setParallelism(parallelism))
			.map(new VertexInverseLogDegree<>())
				.setParallelism(parallelism)
				.name("Vertex score");

		// s, t, 1/log(d(s))
		DataSet<Tuple3<K, K, FloatValue>> sourceInverseLogDegree = input
			.getEdges()
			.join(inverseLogDegree, JoinHint.REPARTITION_HASH_SECOND)
			.where(0)
			.equalTo(0)
			.projectFirst(0, 1)
			.<Tuple3<K, K, FloatValue>>projectSecond(2)
				.setParallelism(parallelism)
				.name("Edge score");

		// group span, s, t, 1/log(d(s))
		DataSet<Tuple4<IntValue, K, K, FloatValue>> groupSpans = sourceInverseLogDegree
			.groupBy(0)
			.sortGroup(1, Order.ASCENDING)
			.reduceGroup(new GenerateGroupSpans<>())
				.setParallelism(parallelism)
				.name("Generate group spans");

		// group, s, t, 1/log(d(s))
		DataSet<Tuple4<IntValue, K, K, FloatValue>> groups = groupSpans
			.rebalance()
				.setParallelism(parallelism)
				.name("Rebalance")
			.flatMap(new GenerateGroups<>())
				.setParallelism(parallelism)
				.name("Generate groups");

		// t, u, 1/log(d(s)) where (s, t) and (s, u) are edges in graph
		DataSet<Tuple3<K, K, FloatValue>> twoPaths = groups
			.groupBy(0, 1)
			.sortGroup(2, Order.ASCENDING)
			.reduceGroup(new GenerateGroupPairs<>())
				.name("Generate group pairs");

		// t, u, adamic-adar score
		GroupReduceOperator<Tuple3<K, K, FloatValue>, Result<K>> scores = twoPaths
			.groupBy(0, 1)
			.reduceGroup(new ComputeScores<>(minimumScore, minimumRatio))
				.name("Compute scores");

		if (minimumRatio > 0.0f) {
			// total score, number of pairs of neighbors
			DataSet<Tuple2<FloatValue, LongValue>> sumOfScoresAndNumberOfNeighborPairs = inverseLogDegree
				.map(new ComputeScoreFromVertex<>())
					.setParallelism(parallelism)
					.name("Average score")
				.sum(0)
				.andSum(1);

			scores
				.withBroadcastSet(sumOfScoresAndNumberOfNeighborPairs, SUM_OF_SCORES_AND_NUMBER_OF_NEIGHBOR_PAIRS);
		}

		if (mirrorResults) {
			return scores
				.flatMap(new MirrorResult<>())
					.name("Mirror results");
		} else {
			return scores;
		}
	}

	/**
	 * Compute the inverse logarithm of the vertex degree. This is computed
	 * before enumerating neighbor pairs since logarithm and division are quite
	 * computationally intensive.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0; 1")
	private static class VertexInverseLogDegree<T>
	implements MapFunction<Vertex<T, LongValue>, Tuple3<T, LongValue, FloatValue>> {
		private Tuple3<T, LongValue, FloatValue> output = new Tuple3<>(null, null, new FloatValue());

		@Override
		public Tuple3<T, LongValue, FloatValue> map(Vertex<T, LongValue> value)
				throws Exception {
			output.f0 = value.f0;
			output.f1 = value.f1;

			long degree = value.f1.getValue();
			// when the degree is one the logarithm is zero so avoid dividing by this value
			float inverseLogDegree = (degree == 1) ? 0.0f : 1.0f / (float) Math.log(value.f1.getValue());
			output.f2.setValue(inverseLogDegree);

			return output;
		}
	}

	/**
	 * @see JaccardIndex.GenerateGroupSpans
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0->1; 1->2; 2->3")
	private static class GenerateGroupSpans<T>
	implements GroupReduceFunction<Tuple3<T, T, FloatValue>, Tuple4<IntValue, T, T, FloatValue>> {
		private IntValue groupSpansValue = new IntValue();

		private Tuple4<IntValue, T, T, FloatValue> output = new Tuple4<>(groupSpansValue, null, null, null);

		@Override
		public void reduce(Iterable<Tuple3<T, T, FloatValue>> values, Collector<Tuple4<IntValue, T, T, FloatValue>> out)
				throws Exception {
			int groupCount = 0;
			int groupSpans = 1;

			groupSpansValue.setValue(groupSpans);

			for (Tuple3<T, T, FloatValue> edge : values) {
				output.f1 = edge.f0;
				output.f2 = edge.f1;
				output.f3 = edge.f2;

				out.collect(output);

				if (++groupCount == GROUP_SIZE) {
					groupCount = 0;
					groupSpansValue.setValue(++groupSpans);
				}
			}
		}
	}

	/**
	 * @see JaccardIndex.GenerateGroups
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("1; 2; 3")
	private static class GenerateGroups<T>
	implements FlatMapFunction<Tuple4<IntValue, T, T, FloatValue>, Tuple4<IntValue, T, T, FloatValue>> {
		@Override
		public void flatMap(Tuple4<IntValue, T, T, FloatValue> value, Collector<Tuple4<IntValue, T, T, FloatValue>> out)
				throws Exception {
			int spans = value.f0.getValue();

			for (int idx = 0; idx < spans; idx++) {
				value.f0.setValue(idx);
				out.collect(value);
			}
		}
	}

	/**
	 * @see JaccardIndex.GenerateGroupPairs
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("3->2")
	private static class GenerateGroupPairs<T extends CopyableValue<T>>
	implements GroupReduceFunction<Tuple4<IntValue, T, T, FloatValue>, Tuple3<T, T, FloatValue>> {
		private Tuple3<T, T, FloatValue> output = new Tuple3<>();

		private boolean initialized = false;

		private List<T> visited = new ArrayList<>(GROUP_SIZE);

		@Override
		public void reduce(Iterable<Tuple4<IntValue, T, T, FloatValue>> values, Collector<Tuple3<T, T, FloatValue>> out)
				throws Exception {
			int visitedCount = 0;

			for (Tuple4<IntValue, T, T, FloatValue> edge : values) {
				output.f1 = edge.f2;
				output.f2 = edge.f3;

				for (int i = 0; i < visitedCount; i++) {
					output.f0 = visited.get(i);
					out.collect(output);
				}

				if (visitedCount < GROUP_SIZE) {
					if (!initialized) {
						initialized = true;

						for (int i = 0; i < GROUP_SIZE; i++) {
							visited.add(edge.f2.copy());
						}
					} else {
						edge.f2.copyTo(visited.get(visitedCount));
					}

					visitedCount += 1;
				}
			}
		}
	}

	/**
	 * Compute the sum of scores emitted by the vertex over all pairs of neighbors.
	 *
	 * @param <T> ID type
	 */
	private static class ComputeScoreFromVertex<T>
	implements MapFunction<Tuple3<T, LongValue, FloatValue>, Tuple2<FloatValue, LongValue>> {
		private FloatValue sumOfScores = new FloatValue();

		private LongValue numberOfNeighborPairs = new LongValue();

		private Tuple2<FloatValue, LongValue> output = new Tuple2<>(sumOfScores, numberOfNeighborPairs);

		@Override
		public Tuple2<FloatValue, LongValue> map(Tuple3<T, LongValue, FloatValue> value) throws Exception {
			long degree = value.f1.getValue();
			long neighborPairs = degree * (degree - 1) / 2;

			sumOfScores.setValue(value.f2.getValue() * neighborPairs);
			numberOfNeighborPairs.setValue(neighborPairs);

			return output;
		}
	}

	/**
	 * Compute the Adamic-Adar similarity as the sum over common neighbors of
	 * the inverse logarithm of degree.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0->vertexId0; 1->vertexId1")
	private static class ComputeScores<T>
	extends RichGroupReduceFunction<Tuple3<T, T, FloatValue>, Result<T>> {
		private float minimumScore;

		private float minimumRatio;

		private Result<T> output = new Result<>();

		public ComputeScores(float minimumScore, float minimumRatio) {
			this.minimumScore = minimumScore;
			this.minimumRatio = minimumRatio;
		}

		@Override
		public void open(Configuration parameters)
				throws Exception {
			super.open(parameters);

			if (minimumRatio > 0.0f) {
				Collection<Tuple2<FloatValue, LongValue>> var;
				var = getRuntimeContext().getBroadcastVariable(SUM_OF_SCORES_AND_NUMBER_OF_NEIGHBOR_PAIRS);
				Tuple2<FloatValue, LongValue> sumAndCount = var.iterator().next();

				float averageScore = sumAndCount.f0.getValue() / sumAndCount.f1.getValue();
				minimumScore = Math.max(minimumScore, averageScore * minimumRatio);
			}
		}

		@Override
		public void reduce(Iterable<Tuple3<T, T, FloatValue>> values, Collector<Result<T>> out)
				throws Exception {
			double sum = 0;
			Tuple3<T, T, FloatValue> edge = null;

			for (Tuple3<T, T, FloatValue> next : values) {
				edge = next;
				sum += next.f2.getValue();
			}

			if (sum >= minimumScore) {
				output.setVertexId0(edge.f0);
				output.setVertexId1(edge.f1);
				output.setAdamicAdarScore((float) sum);
				out.collect(output);
			}
		}
	}

	/**
	 * A result for the Adamic-Adar algorithm.
	 *
	 * @param <T> ID type
	 */
	public static class Result<T>
	extends BinaryResultBase<T>
	implements PrintableResult, Comparable<Result<T>> {
		private FloatValue adamicAdarScore = new FloatValue();

		/**
		 * Get the Adamic-Adar score, equal to the sum over common neighbors of
		 * the inverse logarithm of degree.
		 *
		 * @return Adamic-Adar score
		 */
		public FloatValue getAdamicAdarScore() {
			return adamicAdarScore;
		}

		/**
		 * Set the Adamic-Adar score, equal to the sum over common neighbors of
		 * the inverse logarithm of degree.
		 *
		 * @param adamicAdarScore the Adamic-Adar score
		 */
		public void setAdamicAdarScore(FloatValue adamicAdarScore) {
			this.adamicAdarScore = adamicAdarScore;
		}

		private void setAdamicAdarScore(float adamicAdarScore) {
			this.adamicAdarScore.setValue(adamicAdarScore);
		}

		@Override
		public String toString() {
			return "(" + getVertexId0()
				+ "," + getVertexId1()
				+ "," + adamicAdarScore
				+ ")";
		}

		@Override
		public String toPrintableString() {
			return "Vertex IDs: (" + getVertexId0()
				+ ", " + getVertexId1()
				+ "), adamic-adar score: " + adamicAdarScore;
		}

		@Override
		public int compareTo(Result<T> o) {
			return Float.compare(adamicAdarScore.getValue(), o.adamicAdarScore.getValue());
		}

		// ----------------------------------------------------------------------------------------

		public static final int HASH_SEED = 0xe405f6d1;

		private transient MurmurHash hasher;

		@Override
		public int hashCode() {
			if (hasher == null) {
				hasher = new MurmurHash(HASH_SEED);
			}

			return hasher.reset()
				.hash(getVertexId0().hashCode())
				.hash(getVertexId1().hashCode())
				.hash(adamicAdarScore.getValue())
				.hash();
		}
	}
}
