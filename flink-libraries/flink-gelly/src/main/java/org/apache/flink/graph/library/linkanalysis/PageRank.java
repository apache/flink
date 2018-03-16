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

package org.apache.flink.graph.library.linkanalysis;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.DoubleSumAggregator;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.EdgeSourceDegrees;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees.Degrees;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.asm.result.UnaryResultBase;
import org.apache.flink.graph.library.linkanalysis.Functions.SumScore;
import org.apache.flink.graph.library.linkanalysis.PageRank.Result;
import org.apache.flink.graph.utils.GraphUtils;
import org.apache.flink.graph.utils.MurmurHash;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Iterator;

/**
 * PageRank computes a per-vertex score which is the sum of PageRank scores
 * transmitted over in-edges. Each vertex's score is divided evenly among
 * out-edges. High-scoring vertices are linked to by other high-scoring
 * vertices; this is similar to the 'authority' score in {@link HITS}.
 *
 * <p>See http://ilpubs.stanford.edu:8090/422/1/1999-66.pdf
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class PageRank<K, VV, EV>
extends GraphAlgorithmWrappingDataSet<K, VV, EV, Result<K>> {

	private static final String VERTEX_COUNT = "vertex count";

	private static final String SUM_OF_SCORES = "sum of scores";

	private static final String CHANGE_IN_SCORES = "change in scores";

	// Required configuration
	private final double dampingFactor;

	private int maxIterations;

	private double convergenceThreshold;

	// Optional configuration
	private boolean includeZeroDegreeVertices = false;

	/**
	 * PageRank with a fixed number of iterations.
	 *
	 * @param dampingFactor probability of following an out-link, otherwise jump to a random vertex
	 * @param iterations fixed number of iterations
	 */
	public PageRank(double dampingFactor, int iterations) {
		this(dampingFactor, iterations, Double.MAX_VALUE);
	}

	/**
	 * PageRank with a convergence threshold. The algorithm terminates when the
	 * change in score over all vertices falls to or below the given threshold value.
	 *
	 * @param dampingFactor probability of following an out-link, otherwise jump to a random vertex
	 * @param convergenceThreshold convergence threshold for sum of scores
	 */
	public PageRank(double dampingFactor, double convergenceThreshold) {
		this(dampingFactor, Integer.MAX_VALUE, convergenceThreshold);
	}

	/**
	 * PageRank with a convergence threshold and a maximum iteration count. The
	 * algorithm terminates after either the given number of iterations or when
	 * the change in score over all vertices falls to or below the given
	 * threshold value.
	 *
	 * @param dampingFactor probability of following an out-link, otherwise jump to a random vertex
	 * @param maxIterations maximum number of iterations
	 * @param convergenceThreshold convergence threshold for sum of scores
	 */
	public PageRank(double dampingFactor, int maxIterations, double convergenceThreshold) {
		Preconditions.checkArgument(0 < dampingFactor && dampingFactor < 1,
			"Damping factor must be between zero and one");
		Preconditions.checkArgument(maxIterations > 0, "Number of iterations must be greater than zero");
		Preconditions.checkArgument(convergenceThreshold > 0.0, "Convergence threshold must be greater than zero");

		this.dampingFactor = dampingFactor;
		this.maxIterations = maxIterations;
		this.convergenceThreshold = convergenceThreshold;
	}

	/**
	 * This PageRank implementation properly handles both source and sink
	 * vertices which have, respectively, only outgoing and incoming edges.
	 *
	 * <p>Setting this flag includes "zero-degree" vertices in the PageRank
	 * computation and result. These vertices are handled the same as other
	 * "source" vertices (with a consistent score of
	 * <code>(1 - damping factor) / number of vertices</code>) but only
	 * affect the scores of other vertices indirectly through the taking of
	 * this proportional portion of the "random jump" score.
	 *
	 * <p>The cost to include zero-degree vertices is a reduce for uniqueness
	 * on the vertex set followed by an outer join on the vertex degree
	 * DataSet.
	 *
	 * @param includeZeroDegreeVertices whether to include zero-degree vertices in the iterative computation
	 * @return this
	 */
	public PageRank<K, VV, EV> setIncludeZeroDegreeVertices(boolean includeZeroDegreeVertices) {
		this.includeZeroDegreeVertices = includeZeroDegreeVertices;

		return this;
	}

	@Override
	protected boolean canMergeConfigurationWith(GraphAlgorithmWrappingBase other) {
		if (!super.canMergeConfigurationWith(other)) {
			return false;
		}

		PageRank rhs = (PageRank) other;

		return dampingFactor == rhs.dampingFactor &&
			includeZeroDegreeVertices == rhs.includeZeroDegreeVertices;
	}

	@Override
	protected void mergeConfiguration(GraphAlgorithmWrappingBase other) {
		super.mergeConfiguration(other);

		PageRank rhs = (PageRank) other;

		maxIterations = Math.max(maxIterations, rhs.maxIterations);
		convergenceThreshold = Math.min(convergenceThreshold, rhs.convergenceThreshold);
	}

	@Override
	public DataSet<Result<K>> runInternal(Graph<K, VV, EV> input)
			throws Exception {
		// vertex degree
		DataSet<Vertex<K, Degrees>> vertexDegree = input
			.run(new VertexDegrees<K, VV, EV>()
				.setIncludeZeroDegreeVertices(includeZeroDegreeVertices)
				.setParallelism(parallelism));

		// vertex count
		DataSet<LongValue> vertexCount = GraphUtils.count(vertexDegree);

		// s, t, d(s)
		DataSet<Edge<K, LongValue>> edgeSourceDegree = input
			.run(new EdgeSourceDegrees<K, VV, EV>()
				.setParallelism(parallelism))
			.map(new ExtractSourceDegree<>())
				.setParallelism(parallelism)
				.name("Extract source degree");

		// vertices with zero in-edges
		DataSet<Tuple2<K, DoubleValue>> sourceVertices = vertexDegree
			.flatMap(new InitializeSourceVertices<>())
				.setParallelism(parallelism)
				.name("Initialize source vertex scores");

		// s, initial pagerank(s)
		DataSet<Tuple2<K, DoubleValue>> initialScores = vertexDegree
			.map(new InitializeVertexScores<>())
			.withBroadcastSet(vertexCount, VERTEX_COUNT)
				.setParallelism(parallelism)
				.name("Initialize scores");

		IterativeDataSet<Tuple2<K, DoubleValue>> iterative = initialScores
			.iterate(maxIterations)
			.setParallelism(parallelism);

		// s, projected pagerank(s)
		DataSet<Tuple2<K, DoubleValue>> vertexScores = iterative
			.coGroup(edgeSourceDegree)
			.where(0)
			.equalTo(0)
			.with(new SendScore<>())
				.setParallelism(parallelism)
				.name("Send score")
			.groupBy(0)
			.reduce(new SumScore<>())
			.setCombineHint(CombineHint.HASH)
				.setParallelism(parallelism)
				.name("Sum");

		// ignored ID, total pagerank
		DataSet<Tuple2<K, DoubleValue>> sumOfScores = vertexScores
			.reduce(new SumVertexScores<>())
				.setParallelism(parallelism)
				.name("Sum");

		// s, adjusted pagerank(s)
		DataSet<Tuple2<K, DoubleValue>> adjustedScores = vertexScores
			.union(sourceVertices)
				.setParallelism(parallelism)
				.name("Union with source vertices")
			.map(new AdjustScores<>(dampingFactor))
				.withBroadcastSet(sumOfScores, SUM_OF_SCORES)
				.withBroadcastSet(vertexCount, VERTEX_COUNT)
					.setParallelism(parallelism)
					.name("Adjust scores");

		DataSet<Tuple2<K, DoubleValue>> passThrough;

		if (convergenceThreshold < Double.MAX_VALUE) {
			passThrough = iterative
				.join(adjustedScores)
				.where(0)
				.equalTo(0)
				.with(new ChangeInScores<>())
					.setParallelism(parallelism)
					.name("Change in scores");

			iterative.registerAggregationConvergenceCriterion(CHANGE_IN_SCORES, new DoubleSumAggregator(), new ScoreConvergence(convergenceThreshold));
		} else {
			passThrough = adjustedScores;
		}

		return iterative
			.closeWith(passThrough)
			.map(new TranslateResult<>())
				.setParallelism(parallelism)
				.name("Map result");
	}

	/**
	 * Remove the unused original edge value and extract the out-degree.
	 *
	 * @param <T> ID type
	 * @param <ET> edge value type
	 */
	@ForwardedFields("0; 1")
	private static class ExtractSourceDegree<T, ET>
	implements MapFunction<Edge<T, Tuple2<ET, Degrees>>, Edge<T, LongValue>> {
		Edge<T, LongValue> output = new Edge<>();

		@Override
		public Edge<T, LongValue> map(Edge<T, Tuple2<ET, Degrees>> edge)
				throws Exception {
			output.f0 = edge.f0;
			output.f1 = edge.f1;
			output.f2 = edge.f2.f1.getOutDegree();
			return output;
		}
	}

	/**
	 * Source vertices have no in-edges so have a projected score of 0.0.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0")
	private static class InitializeSourceVertices<T>
	implements FlatMapFunction<Vertex<T, Degrees>, Tuple2<T, DoubleValue>> {
		private Tuple2<T, DoubleValue> output = new Tuple2<>(null, new DoubleValue(0.0));

		@Override
		public void flatMap(Vertex<T, Degrees> vertex, Collector<Tuple2<T, DoubleValue>> out)
				throws Exception {
			if (vertex.f1.getInDegree().getValue() == 0) {
				output.f0 = vertex.f0;
				out.collect(output);
			}
		}
	}

	/**
	 * PageRank scores sum to 1.0 so initialize each vertex with the inverse of
	 * the number of vertices.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0")
	private static class InitializeVertexScores<T>
	extends RichMapFunction<Vertex<T, Degrees>, Tuple2<T, DoubleValue>> {
		private Tuple2<T, DoubleValue> output = new Tuple2<>();

		@Override
		public void open(Configuration parameters)
				throws Exception {
			super.open(parameters);

			Collection<LongValue> vertexCount = getRuntimeContext().getBroadcastVariable(VERTEX_COUNT);
			Iterator<LongValue> vertexCountIterator = vertexCount.iterator();
			output.f1 = new DoubleValue(vertexCountIterator.hasNext() ? 1.0 / vertexCountIterator.next().getValue() : Double.NaN);
		}

		@Override
		public Tuple2<T, DoubleValue> map(Vertex<T, Degrees> vertex)
				throws Exception {
			output.f0 = vertex.f0;
			return output;
		}
	}

	/**
	 * The PageRank score for each vertex is divided evenly and projected to
	 * neighbors on out-edges.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFieldsSecond("1->0")
	private static class SendScore<T>
	implements CoGroupFunction<Tuple2<T, DoubleValue>, Edge<T, LongValue>, Tuple2<T, DoubleValue>> {
		private Tuple2<T, DoubleValue> output = new Tuple2<>(null, new DoubleValue());

		@Override
		public void coGroup(Iterable<Tuple2<T, DoubleValue>> vertex, Iterable<Edge<T, LongValue>> edges, Collector<Tuple2<T, DoubleValue>> out)
				throws Exception {
			Iterator<Edge<T, LongValue>> edgeIterator = edges.iterator();

			if (edgeIterator.hasNext()) {
				Edge<T, LongValue> edge = edgeIterator.next();

				output.f0 = edge.f1;
				output.f1.setValue(vertex.iterator().next().f1.getValue() / edge.f2.getValue());
				out.collect(output);

				while (edgeIterator.hasNext()) {
					edge = edgeIterator.next();
					output.f0 = edge.f1;
					out.collect(output);
				}
			}
		}
	}

	/**
	 * Sum the PageRank score over all vertices. The vertex ID must be ignored
	 * but is retained rather than adding another operator.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0")
	private static class SumVertexScores<T>
	implements ReduceFunction<Tuple2<T, DoubleValue>> {
		@Override
		public Tuple2<T, DoubleValue> reduce(Tuple2<T, DoubleValue> first, Tuple2<T, DoubleValue> second)
				throws Exception {
			first.f1.setValue(first.f1.getValue() + second.f1.getValue());
			return first;
		}
	}

	/**
	 * Each iteration the per-vertex scores are adjusted with the damping
	 * factor. Each score is multiplied by the damping factor then added to the
	 * probability of a "random hop", which is one minus the damping factor.
	 *
	 * <p>This operation also accounts for 'sink' vertices, which have no
	 * out-edges to project score to. The sink scores are computed by taking
	 * one minus the sum of vertex scores, which also includes precision error.
	 * This 'missing' score is evenly distributed across vertices as with the
	 * random hop.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0")
	private static class AdjustScores<T>
	extends RichMapFunction<Tuple2<T, DoubleValue>, Tuple2<T, DoubleValue>> {
		private double dampingFactor;

		private long vertexCount;

		private double uniformlyDistributedScore;

		public AdjustScores(double dampingFactor) {
			this.dampingFactor = dampingFactor;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			Collection<Tuple2<T, DoubleValue>> sumOfScores = getRuntimeContext().getBroadcastVariable(SUM_OF_SCORES);
			Iterator<Tuple2<T, DoubleValue>> sumOfScoresIterator = sumOfScores.iterator();
			// floating point precision error is also included in sumOfSinks
			double sumOfSinks = 1 - (sumOfScoresIterator.hasNext() ? sumOfScoresIterator.next().f1.getValue() : 0);

			Collection<LongValue> vertexCount = getRuntimeContext().getBroadcastVariable(VERTEX_COUNT);
			Iterator<LongValue> vertexCountIterator = vertexCount.iterator();
			this.vertexCount = vertexCountIterator.hasNext() ? vertexCountIterator.next().getValue() : 0;

			this.uniformlyDistributedScore = ((1 - dampingFactor) + dampingFactor * sumOfSinks) / this.vertexCount;
		}

		@Override
		public Tuple2<T, DoubleValue> map(Tuple2<T, DoubleValue> value) throws Exception {
			value.f1.setValue(uniformlyDistributedScore + (dampingFactor * value.f1.getValue()));
			return value;
		}
	}

	/**
	 * Computes the sum of the absolute change in vertex PageRank scores
	 * between iterations.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFieldsFirst("0")
	@ForwardedFieldsSecond("*")
	private static class ChangeInScores<T>
	extends RichJoinFunction<Tuple2<T, DoubleValue>, Tuple2<T, DoubleValue>, Tuple2<T, DoubleValue>> {
		private double changeInScores;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			changeInScores = 0.0;
		}

		@Override
		public void close()
				throws Exception {
			super.close();

			DoubleSumAggregator agg = getIterationRuntimeContext().getIterationAggregator(CHANGE_IN_SCORES);
			agg.aggregate(changeInScores);
		}

		@Override
		public Tuple2<T, DoubleValue> join(Tuple2<T, DoubleValue> first, Tuple2<T, DoubleValue> second)
				throws Exception {
			changeInScores += Math.abs(second.f1.getValue() - first.f1.getValue());
			return second;
		}
	}

	/**
	 * Monitors the sum of the absolute change in vertex scores. The algorithm
	 * terminates when the change in scores compared against the prior iteration
	 * falls to or below the given convergence threshold.
	 */
	private static class ScoreConvergence
	implements ConvergenceCriterion<DoubleValue> {
		private double convergenceThreshold;

		public ScoreConvergence(double convergenceThreshold) {
			this.convergenceThreshold = convergenceThreshold;
		}

		@Override
		public boolean isConverged(int iteration, DoubleValue value) {
			double val = value.getValue();
			return (val <= convergenceThreshold);
		}
	}

	/**
	 * Map the Tuple result to the return type.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0->vertexId0; 1->pageRankScore")
	private static class TranslateResult<T>
		implements MapFunction<Tuple2<T, DoubleValue>, Result<T>> {
		private Result<T> output = new Result<>();

		@Override
		public Result<T> map(Tuple2<T, DoubleValue> value) throws Exception {
			output.setVertexId0(value.f0);
			output.setPageRankScore(value.f1);
			return output;
		}
	}

	/**
	 * A result for the PageRank algorithm.
	 *
	 * @param <T> ID type
	 */
	public static class Result<T>
	extends UnaryResultBase<T>
	implements PrintableResult {
		private DoubleValue pageRankScore;

		/**
		 * Get the PageRank score.
		 *
		 * @return the PageRank score
		 */
		public DoubleValue getPageRankScore() {
			return pageRankScore;
		}

		/**
		 * Set the PageRank score.
		 *
		 * @param pageRankScore the PageRank score
		 */
		public void setPageRankScore(DoubleValue pageRankScore) {
			this.pageRankScore = pageRankScore;
		}

		@Override
		public String toString() {
			return "(" + getVertexId0()
				+ "," + pageRankScore
				+ ")";
		}

		@Override
		public String toPrintableString() {
			return "Vertex ID: " + getVertexId0()
				+ ", PageRank score: " + pageRankScore;
		}

		// ----------------------------------------------------------------------------------------

		public static final int HASH_SEED = 0x4010af29;

		private transient MurmurHash hasher;

		@Override
		public int hashCode() {
			if (hasher == null) {
				hasher = new MurmurHash(HASH_SEED);
			}

			return hasher.reset()
				.hash(getVertexId0().hashCode())
				.hash(pageRankScore.getValue())
				.hash();
		}
	}
}
