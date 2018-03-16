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
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.degree.annotate.undirected.EdgeTargetDegree;
import org.apache.flink.graph.asm.result.BinaryResult.MirrorResult;
import org.apache.flink.graph.asm.result.BinaryResultBase;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.library.similarity.JaccardIndex.Result;
import org.apache.flink.graph.utils.MurmurHash;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * The Jaccard Index measures the similarity between vertex neighborhoods and
 * is computed as the number of shared neighbors divided by the number of
 * distinct neighbors. Scores range from 0.0 (no shared neighbors) to 1.0 (all
 * neighbors are shared).
 *
 * <p>This implementation produces similarity scores for each pair of vertices
 * in the graph with at least one shared neighbor; equivalently, this is the
 * set of all non-zero Jaccard Similarity coefficients.
 *
 * <p>The input graph must be a simple, undirected graph containing no duplicate
 * edges or self-loops.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class JaccardIndex<K extends CopyableValue<K>, VV, EV>
extends GraphAlgorithmWrappingDataSet<K, VV, EV, Result<K>> {

	public static final int DEFAULT_GROUP_SIZE = 64;

	// Optional configuration
	private int groupSize = DEFAULT_GROUP_SIZE;

	private boolean unboundedScores = true;

	private int minimumScoreNumerator = 0;

	private int minimumScoreDenominator = 1;

	private int maximumScoreNumerator = 1;

	private int maximumScoreDenominator = 1;

	private boolean mirrorResults;

	/**
	 * Override the default group size for the quadratic expansion of neighbor
	 * pairs. Small groups generate more data whereas large groups distribute
	 * computation less evenly among tasks.
	 *
	 * <p>The default value should be near-optimal for all use cases.
	 *
	 * @param groupSize the group size for the quadratic expansion of neighbor pairs
	 * @return this
	 */
	public JaccardIndex<K, VV, EV> setGroupSize(int groupSize) {
		Preconditions.checkArgument(groupSize > 0, "Group size must be greater than zero");

		this.groupSize = groupSize;

		return this;
	}

	/**
	 * Filter out Jaccard Index scores less than the given minimum fraction.
	 *
	 * @param numerator numerator of the minimum score
	 * @param denominator denominator of the minimum score
	 * @return this
	 * @see #setMaximumScore(int, int)
	 */
	public JaccardIndex<K, VV, EV> setMinimumScore(int numerator, int denominator) {
		Preconditions.checkArgument(numerator >= 0, "Minimum score numerator must be non-negative");
		Preconditions.checkArgument(denominator > 0, "Minimum score denominator must be greater than zero");
		Preconditions.checkArgument(numerator <= denominator, "Minimum score fraction must be less than or equal to one");

		this.unboundedScores = false;
		this.minimumScoreNumerator = numerator;
		this.minimumScoreDenominator = denominator;

		return this;
	}

	/**
	 * Filter out Jaccard Index scores greater than the given maximum fraction.
	 *
	 * @param numerator numerator of the maximum score
	 * @param denominator denominator of the maximum score
	 * @return this
	 * @see #setMinimumScore(int, int)
	 */
	public JaccardIndex<K, VV, EV> setMaximumScore(int numerator, int denominator) {
		Preconditions.checkArgument(numerator >= 0, "Maximum score numerator must be non-negative");
		Preconditions.checkArgument(denominator > 0, "Maximum score denominator must be greater than zero");
		Preconditions.checkArgument(numerator <= denominator, "Maximum score fraction must be less than or equal to one");

		this.unboundedScores = false;
		this.maximumScoreNumerator = numerator;
		this.maximumScoreDenominator = denominator;

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
	public JaccardIndex<K, VV, EV> setMirrorResults(boolean mirrorResults) {
		this.mirrorResults = mirrorResults;

		return this;
	}

	@Override
	protected boolean canMergeConfigurationWith(GraphAlgorithmWrappingBase other) {
		if (!super.canMergeConfigurationWith(other)) {
			return false;
		}

		JaccardIndex rhs = (JaccardIndex) other;

		return unboundedScores == rhs.unboundedScores &&
			minimumScoreNumerator == rhs.minimumScoreNumerator &&
			minimumScoreDenominator == rhs.minimumScoreDenominator &&
			maximumScoreNumerator == rhs.maximumScoreNumerator &&
			maximumScoreDenominator == rhs.maximumScoreDenominator &&
			mirrorResults == rhs.mirrorResults;
	}

	@Override
	protected void mergeConfiguration(GraphAlgorithmWrappingBase other) {
		super.mergeConfiguration(other);

		JaccardIndex rhs = (JaccardIndex) other;

		groupSize = Math.max(groupSize, rhs.groupSize);
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
		// s, t, d(t)
		DataSet<Edge<K, Tuple2<EV, LongValue>>> neighborDegree = input
			.run(new EdgeTargetDegree<K, VV, EV>()
				.setParallelism(parallelism));

		// group span, s, t, d(t)
		DataSet<Tuple4<IntValue, K, K, IntValue>> groupSpans = neighborDegree
			.groupBy(0)
			.sortGroup(1, Order.ASCENDING)
			.reduceGroup(new GenerateGroupSpans<>(groupSize))
				.setParallelism(parallelism)
				.name("Generate group spans");

		// group, s, t, d(t)
		DataSet<Tuple4<IntValue, K, K, IntValue>> groups = groupSpans
			.rebalance()
				.setParallelism(parallelism)
				.name("Rebalance")
			.flatMap(new GenerateGroups<>())
				.setParallelism(parallelism)
				.name("Generate groups");

		// t, u, d(t)+d(u)
		DataSet<Tuple3<K, K, IntValue>> twoPaths = groups
			.groupBy(0, 1)
			.sortGroup(2, Order.ASCENDING)
			.reduceGroup(new GenerateGroupPairs<>(groupSize))
				.name("Generate group pairs");

		// t, u, intersection, union
		DataSet<Result<K>> scores = twoPaths
			.groupBy(0, 1)
			.reduceGroup(new ComputeScores<>(unboundedScores,
					minimumScoreNumerator, minimumScoreDenominator,
					maximumScoreNumerator, maximumScoreDenominator))
				.name("Compute scores");

		if (mirrorResults) {
			scores = scores
				.flatMap(new MirrorResult<>())
					.name("Mirror results");
		}

		return scores;
	}

	/**
	 * This is the first of three operations implementing a self-join to generate
	 * the full neighbor pairing for each vertex. The number of neighbor pairs
	 * is (n choose 2) which is quadratic in the vertex degree.
	 *
	 * <p>The third operation, {@link GenerateGroupPairs}, processes groups of size
	 * {@link #groupSize} and emits {@code O(groupSize * deg(vertex))} pairs.
	 *
	 * <p>This input to the third operation is still quadratic in the vertex degree.
	 * Two prior operations, {@link GenerateGroupSpans} and {@link GenerateGroups},
	 * each emit datasets linear in the vertex degree, with a forced rebalance
	 * in between. {@link GenerateGroupSpans} first annotates each edge with the
	 * number of groups and {@link GenerateGroups} emits each edge into each group.
	 *
	 * @param <T> ID type
	 * @param <ET> edge value type
	 */
	@ForwardedFields("0->1; 1->2")
	private static class GenerateGroupSpans<T, ET>
	implements GroupReduceFunction<Edge<T, Tuple2<ET, LongValue>>, Tuple4<IntValue, T, T, IntValue>> {
		private final int groupSize;

		private IntValue groupSpansValue = new IntValue();

		private Tuple4<IntValue, T, T, IntValue> output = new Tuple4<>(groupSpansValue, null, null, new IntValue());

		public GenerateGroupSpans(int groupSize) {
			this.groupSize = groupSize;
		}

		@Override
		public void reduce(Iterable<Edge<T, Tuple2<ET, LongValue>>> values, Collector<Tuple4<IntValue, T, T, IntValue>> out)
				throws Exception {
			int groupCount = 0;
			int groupSpans = 1;

			groupSpansValue.setValue(groupSpans);

			for (Edge<T, Tuple2<ET, LongValue>> edge : values) {
				long degree = edge.f2.f1.getValue();
				if (degree > Integer.MAX_VALUE) {
					throw new RuntimeException("Degree overflows IntValue");
				}

				// group span, u, v, d(v)
				output.f1 = edge.f0;
				output.f2 = edge.f1;
				output.f3.setValue((int) degree);

				out.collect(output);

				if (++groupCount == groupSize) {
					groupCount = 0;
					groupSpansValue.setValue(++groupSpans);
				}
			}
		}
	}

	/**
	 * Emits the input tuple into each group within its group span.
	 *
	 * @param <T> ID type
	 *
	 * @see GenerateGroupSpans
	 */
	@ForwardedFields("1; 2; 3")
	private static class GenerateGroups<T>
	implements FlatMapFunction<Tuple4<IntValue, T, T, IntValue>, Tuple4<IntValue, T, T, IntValue>> {
		@Override
		public void flatMap(Tuple4<IntValue, T, T, IntValue> value, Collector<Tuple4<IntValue, T, T, IntValue>> out)
				throws Exception {
			int spans = value.f0.getValue();

			for (int idx = 0; idx < spans; idx++) {
				value.f0.setValue(idx);
				out.collect(value);
			}
		}
	}

	/**
	 * Emits the two-path for all neighbor pairs in this group.
	 *
	 * <p>The first {@link #groupSize} vertices are emitted pairwise. Following
	 * vertices are only paired with vertices from this initial group.
	 *
	 * @param <T> ID type
	 *
	 * @see GenerateGroupSpans
	 */
	private static class GenerateGroupPairs<T extends CopyableValue<T>>
	implements GroupReduceFunction<Tuple4<IntValue, T, T, IntValue>, Tuple3<T, T, IntValue>> {
		private final int groupSize;

		private boolean initialized = false;

		private List<Tuple3<T, T, IntValue>> visited;

		public GenerateGroupPairs(int groupSize) {
			this.groupSize = groupSize;
			this.visited = new ArrayList<>(groupSize);
		}

		@Override
		public void reduce(Iterable<Tuple4<IntValue, T, T, IntValue>> values, Collector<Tuple3<T, T, IntValue>> out)
				throws Exception {
			int visitedCount = 0;

			for (Tuple4<IntValue, T, T, IntValue> edge : values) {
				for (int i = 0; i < visitedCount; i++) {
					Tuple3<T, T, IntValue> prior = visited.get(i);

					prior.f1 = edge.f2;

					int oldValue = prior.f2.getValue();

					long degreeSum = oldValue + edge.f3.getValue();
					if (degreeSum > Integer.MAX_VALUE) {
						throw new RuntimeException("Degree sum overflows IntValue");
					}
					prior.f2.setValue((int) degreeSum);

					// v, w, d(v) + d(w)
					out.collect(prior);

					prior.f2.setValue(oldValue);
				}

				if (visitedCount < groupSize) {
					if (!initialized) {
						initialized = true;

						for (int i = 0; i < groupSize; i++) {
							Tuple3<T, T, IntValue> tuple = new Tuple3<>();

							tuple.f0 = edge.f2.copy();
							tuple.f2 = edge.f3.copy();

							visited.add(tuple);
						}
					} else {
						Tuple3<T, T, IntValue> copy = visited.get(visitedCount);

						edge.f2.copyTo(copy.f0);
						edge.f3.copyTo(copy.f2);
					}

					visitedCount += 1;
				}
			}
		}
	}

	/**
	 * Compute the counts of shared and distinct neighbors. A two-path connecting
	 * the vertices is emitted for each shared neighbor. The number of distinct
	 * neighbors is equal to the sum of degrees of the vertices minus the count
	 * of shared numbers, which are double-counted in the degree sum.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0->vertexId0; 1->vertexId1")
	private static class ComputeScores<T>
	implements GroupReduceFunction<Tuple3<T, T, IntValue>, Result<T>> {
		private boolean unboundedScores;

		private long minimumScoreNumerator;

		private long minimumScoreDenominator;

		private long maximumScoreNumerator;

		private long maximumScoreDenominator;

		private Result<T> output = new Result<>();

		public ComputeScores(boolean unboundedScores,
				int minimumScoreNumerator, int minimumScoreDenominator,
				int maximumScoreNumerator, int maximumScoreDenominator) {
			this.unboundedScores = unboundedScores;
			this.minimumScoreNumerator = minimumScoreNumerator;
			this.minimumScoreDenominator = minimumScoreDenominator;
			this.maximumScoreNumerator = maximumScoreNumerator;
			this.maximumScoreDenominator = maximumScoreDenominator;
		}

		@Override
		public void reduce(Iterable<Tuple3<T, T, IntValue>> values, Collector<Result<T>> out)
				throws Exception {
			int count = 0;
			Tuple3<T, T, IntValue> edge = null;

			for (Tuple3<T, T, IntValue> next : values) {
				edge = next;
				count += 1;
			}

			int distinctNeighbors = edge.f2.getValue() - count;

			if (unboundedScores ||
					(count * minimumScoreDenominator >= distinctNeighbors * minimumScoreNumerator
						&& count * maximumScoreDenominator <= distinctNeighbors * maximumScoreNumerator)) {
				output.setVertexId0(edge.f0);
				output.setVertexId1(edge.f1);
				output.setSharedNeighborCount(count);
				output.setDistinctNeighborCount(distinctNeighbors);
				out.collect(output);
			}
		}
	}

	/**
	 * A result for the Jaccard Index algorithm.
	 *
	 * @param <T> ID type
	 */
	public static class Result<T>
	extends BinaryResultBase<T>
	implements PrintableResult, Comparable<Result<T>> {
		private IntValue sharedNeighborCount = new IntValue();

		private IntValue distinctNeighborCount = new IntValue();

		/**
		 * Get the shared neighbor count.
		 *
		 * @return shared neighbor count
		 */
		public IntValue getSharedNeighborCount() {
			return sharedNeighborCount;
		}

		/**
		 * Set the shared neighbor count.
		 *
		 * @param sharedNeighborCount the shared neighbor count
		 */
		public void setSharedNeighborCount(IntValue sharedNeighborCount) {
			this.sharedNeighborCount = sharedNeighborCount;
		}

		private void setSharedNeighborCount(int sharedNeighborCount) {
			this.sharedNeighborCount.setValue(sharedNeighborCount);
		}

		/**
		 * Get the distinct neighbor count.
		 *
		 * @return distinct neighbor count
		 */
		public IntValue getDistinctNeighborCount() {
			return distinctNeighborCount;
		}

		/**
		 * Set the distinct neighbor count.
		 *
		 * @param distinctNeighborCount the distinct neighbor count
		 */
		public void setDistinctNeighborCount(IntValue distinctNeighborCount) {
			this.distinctNeighborCount = distinctNeighborCount;
		}

		private void setDistinctNeighborCount(int distinctNeighborCount) {
			this.distinctNeighborCount.setValue(distinctNeighborCount);
		}

		/**
		 * Get the Jaccard Index score, equal to the number of shared neighbors
		 * of the source and target vertices divided by the number of distinct
		 * neighbors.
		 *
		 * @return Jaccard Index score
		 */
		public double getJaccardIndexScore() {
			return getSharedNeighborCount().getValue() / (double) getDistinctNeighborCount().getValue();
		}

		@Override
		public String toString() {
			return "(" + getVertexId0()
				+ "," + getVertexId1()
				+ "," + getSharedNeighborCount()
				+ "," + getDistinctNeighborCount()
				+ ")";
		}

		@Override
		public String toPrintableString() {
			return "Vertex IDs: (" + getVertexId0() + ", " + getVertexId1()
				+ "), number of shared neighbors: " + getSharedNeighborCount()
				+ ", number of distinct neighbors: " + getDistinctNeighborCount()
				+ ", jaccard index score: " + getJaccardIndexScore();
		}

		@Override
		public int compareTo(Result<T> o) {
			// exact comparison of a/b with x/y using only integer math:
			// a/b <?> x/y == a*y <?> b*x

			long ay = sharedNeighborCount.getValue() * (long) o.distinctNeighborCount.getValue();
			long bx = distinctNeighborCount.getValue() * (long) o.sharedNeighborCount.getValue();

			return Long.compare(ay, bx);
		}

		// ----------------------------------------------------------------------------------------

		public static final int HASH_SEED = 0x731f73e7;

		private transient MurmurHash hasher;

		@Override
		public int hashCode() {
			if (hasher == null) {
				hasher = new MurmurHash(HASH_SEED);
			}

			return hasher.reset()
				.hash(getVertexId0().hashCode())
				.hash(getVertexId1().hashCode())
				.hash(sharedNeighborCount.getValue())
				.hash(distinctNeighborCount.getValue())
				.hash();
		}
	}
}
