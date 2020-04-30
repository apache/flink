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

package org.apache.flink.graph.library.clustering.directed;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.graph.AnalyticHelper;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalyticBase;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees.Degrees;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.library.clustering.directed.TriadicCensus.Result;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.IOException;
import java.math.BigInteger;
import java.text.NumberFormat;

/**
 * A triad is formed by three connected or unconnected vertices in a graph.
 * The triadic census counts the occurrences of each type of triad.
 *
 * <p>See http://vlado.fmf.uni-lj.si/pub/networks/doc/triads/triads.pdf
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class TriadicCensus<K extends Comparable<K> & CopyableValue<K>, VV, EV>
extends GraphAnalyticBase<K, VV, EV, Result> {

	private TriangleListingHelper<K> triangleListingHelper;

	private VertexDegreesHelper<K> vertexDegreesHelper;

	@Override
	public TriadicCensus<K, VV, EV> run(Graph<K, VV, EV> input)
			throws Exception {
		super.run(input);

		triangleListingHelper = new TriangleListingHelper<>();

		input
			.run(new TriangleListing<K, VV, EV>()
				.setParallelism(parallelism))
			.output(triangleListingHelper)
				.name("Triangle counts");

		vertexDegreesHelper = new VertexDegreesHelper<>();

		input
			.run(new VertexDegrees<K, VV, EV>()
				.setParallelism(parallelism))
			.output(vertexDegreesHelper)
				.name("Edge and triplet counts");

		return this;
	}

	@Override
	public Result getResult() {
		BigInteger one = BigInteger.ONE;
		BigInteger two = BigInteger.valueOf(2);
		BigInteger three = BigInteger.valueOf(3);
		BigInteger six = BigInteger.valueOf(6);

		BigInteger vertexCount = BigInteger.valueOf(vertexDegreesHelper.<Long>getAccumulator(env, "vc"));
		BigInteger unidirectionalEdgeCount = BigInteger.valueOf(vertexDegreesHelper.<Long>getAccumulator(env, "uec") / 2);
		BigInteger bidirectionalEdgeCount = BigInteger.valueOf(vertexDegreesHelper.<Long>getAccumulator(env, "bec") / 2);
		BigInteger triplet021dCount = BigInteger.valueOf(vertexDegreesHelper.<Long>getAccumulator(env, "021d"));
		BigInteger triplet021uCount = BigInteger.valueOf(vertexDegreesHelper.<Long>getAccumulator(env, "021u"));
		BigInteger triplet021cCount = BigInteger.valueOf(vertexDegreesHelper.<Long>getAccumulator(env, "021c"));
		BigInteger triplet111dCount = BigInteger.valueOf(vertexDegreesHelper.<Long>getAccumulator(env, "111d"));
		BigInteger triplet111uCount = BigInteger.valueOf(vertexDegreesHelper.<Long>getAccumulator(env, "111u"));
		BigInteger triplet201Count = BigInteger.valueOf(vertexDegreesHelper.<Long>getAccumulator(env, "201"));

		// triads with three connecting edges = closed triplet = triangle
		BigInteger triangle030tCount = BigInteger.valueOf(triangleListingHelper.<Long>getAccumulator(env, "030t"));
		BigInteger triangle030cCount = BigInteger.valueOf(triangleListingHelper.<Long>getAccumulator(env, "030c"));
		BigInteger triangle120dCount = BigInteger.valueOf(triangleListingHelper.<Long>getAccumulator(env, "120d"));
		BigInteger triangle120uCount = BigInteger.valueOf(triangleListingHelper.<Long>getAccumulator(env, "120u"));
		BigInteger triangle120cCount = BigInteger.valueOf(triangleListingHelper.<Long>getAccumulator(env, "120c"));
		BigInteger triangle210Count = BigInteger.valueOf(triangleListingHelper.<Long>getAccumulator(env, "210"));
		BigInteger triangle300Count = BigInteger.valueOf(triangleListingHelper.<Long>getAccumulator(env, "300"));

		// triads with two connecting edges = open triplet;
		// each triangle deducts the count of three triplets
		triplet201Count = triplet201Count.subtract(triangle300Count.multiply(three));

		triplet201Count = triplet201Count.subtract(triangle210Count);
		triplet111dCount = triplet111dCount.subtract(triangle210Count);
		triplet111uCount = triplet111uCount.subtract(triangle210Count);

		triplet111dCount = triplet111dCount.subtract(triangle120cCount);
		triplet111uCount = triplet111uCount.subtract(triangle120cCount);
		triplet021cCount = triplet021cCount.subtract(triangle120cCount);

		triplet111uCount = triplet111uCount.subtract(triangle120uCount.multiply(two));
		triplet021uCount = triplet021uCount.subtract(triangle120uCount);

		triplet111dCount = triplet111dCount.subtract(triangle120dCount.multiply(two));
		triplet021dCount = triplet021dCount.subtract(triangle120dCount);

		triplet021cCount = triplet021cCount.subtract(triangle030cCount.multiply(three));

		triplet021cCount = triplet021cCount.subtract(triangle030tCount);
		triplet021uCount = triplet021uCount.subtract(triangle030tCount);
		triplet021dCount = triplet021dCount.subtract(triangle030tCount);

		// triads with one connecting edge; each edge pairs with `vertex count - 2` vertices;
		// each triangle deducts from three and each open triplet from two edges
		BigInteger edge102 = bidirectionalEdgeCount
			.multiply(vertexCount.subtract(two))
			.subtract(triplet111dCount)
			.subtract(triplet111uCount)
			.subtract(triplet201Count.multiply(two))
			.subtract(triangle120dCount)
			.subtract(triangle120uCount)
			.subtract(triangle120cCount)
			.subtract(triangle210Count.multiply(two))
			.subtract(triangle300Count.multiply(three));

		BigInteger edge012 = unidirectionalEdgeCount
			.multiply(vertexCount.subtract(two))
			.subtract(triplet021dCount.multiply(two))
			.subtract(triplet021uCount.multiply(two))
			.subtract(triplet021cCount.multiply(two))
			.subtract(triplet111dCount)
			.subtract(triplet111uCount)
			.subtract(triangle030tCount.multiply(three))
			.subtract(triangle030cCount.multiply(three))
			.subtract(triangle120dCount.multiply(two))
			.subtract(triangle120uCount.multiply(two))
			.subtract(triangle120cCount.multiply(two))
			.subtract(triangle210Count);

		// triads with zero connecting edges;
		// (vertex count choose 3) minus earlier counts
		BigInteger triad003 = vertexCount
			.multiply(vertexCount.subtract(one))
			.multiply(vertexCount.subtract(two))
			.divide(six)
			.subtract(edge012)
			.subtract(edge102)
			.subtract(triplet021dCount)
			.subtract(triplet021uCount)
			.subtract(triplet021cCount)
			.subtract(triplet111dCount)
			.subtract(triplet111uCount)
			.subtract(triangle030tCount)
			.subtract(triangle030cCount)
			.subtract(triplet201Count)
			.subtract(triangle120dCount)
			.subtract(triangle120uCount)
			.subtract(triangle120cCount)
			.subtract(triangle210Count)
			.subtract(triangle300Count);

		return new Result(triad003, edge012, edge102, triplet021dCount,
			triplet021uCount, triplet021cCount, triplet111dCount, triplet111uCount,
			triangle030tCount, triangle030cCount, triplet201Count, triangle120dCount,
			triangle120uCount, triangle120cCount, triangle210Count, triangle300Count);
	}

	/**
	 * Helper class to collect triadic census metrics from the triangle listing.
	 *
	 * @param <T> ID type
	 */
	private static class TriangleListingHelper<T>
	extends AnalyticHelper<TriangleListing.Result<T>> {
		private long[] triangleCount = new long[64];

		@Override
		public void writeRecord(TriangleListing.Result<T> record) throws IOException {
			triangleCount[record.getBitmask().getValue()]++;
		}

		@Override
		public void close() throws IOException {
			// see table from Batagelj and Mrvar, "A subquadratic triad census algorithm for large
			// sparse networks with small maximum degree" (this Flink algorithm does not use their
			// algorithm as we do not assume a small maximum degree)
			int[] typeTable = new int[]{
				1, 2, 2, 3, 2, 4, 6, 8,
				2, 6, 5, 7, 3, 8, 7, 11,
				2, 6, 4, 8, 5, 9, 9, 13,
				6, 10, 9, 14, 7, 14, 12, 15,
				2, 5, 6, 7, 6, 9, 10, 14,
				4, 9, 9, 12, 8, 13, 14, 15,
				3, 7, 8, 11, 7, 12, 14, 15,
				8, 14, 13, 15, 11, 15, 15, 16};

			long triangle030tCount = 0;
			long triangle030cCount = 0;
			long triangle120dCount = 0;
			long triangle120uCount = 0;
			long triangle120cCount = 0;
			long triangle210Count = 0;
			long triangle300tCount = 0;

			for (int i = 0; i < typeTable.length; i++) {
				if (typeTable[i] == 9) {
					triangle030tCount += triangleCount[i];
				} else if (typeTable[i] == 10) {
					triangle030cCount += triangleCount[i];
				} else if (typeTable[i] == 12) {
					triangle120dCount += triangleCount[i];
				} else if (typeTable[i] == 13) {
					triangle120uCount += triangleCount[i];
				} else if (typeTable[i] == 14) {
					triangle120cCount += triangleCount[i];
				} else if (typeTable[i] == 15) {
					triangle210Count += triangleCount[i];
				} else if (typeTable[i] == 16) {
					triangle300tCount += triangleCount[i];
				} else {
					assert triangleCount[i] == 0;
				}
			}

			addAccumulator("030t", new LongCounter(triangle030tCount));
			addAccumulator("030c", new LongCounter(triangle030cCount));
			addAccumulator("120d", new LongCounter(triangle120dCount));
			addAccumulator("120u", new LongCounter(triangle120uCount));
			addAccumulator("120c", new LongCounter(triangle120cCount));
			addAccumulator("210", new LongCounter(triangle210Count));
			addAccumulator("300", new LongCounter(triangle300tCount));
		}
	}

	/**
	 * Helper class to collect triadic census metrics from vertex degrees.
	 *
	 * @param <T> ID type
	 */
	private static class VertexDegreesHelper<T>
	extends AnalyticHelper<Vertex<T, Degrees>> {
		private long vertexCount;
		private long unidirectionalEdgeCount;
		private long bidirectionalEdgeCount;
		private long triplet021dCount;
		private long triplet021uCount;
		private long triplet021cCount;
		private long triplet111dCount;
		private long triplet111uCount;
		private long triplet201Count;

		@Override
		public void writeRecord(Vertex<T, Degrees> record) throws IOException {
			long degree = record.f1.getDegree().getValue();
			long outDegree = record.f1.getOutDegree().getValue();
			long inDegree = record.f1.getInDegree().getValue();

			long unidirectionalEdgesAsSource = degree - inDegree;
			long unidirectionalEdgesAsTarget = degree - outDegree;
			long bidirectionalEdges = inDegree + outDegree - degree;

			vertexCount++;
			unidirectionalEdgeCount += unidirectionalEdgesAsSource + unidirectionalEdgesAsTarget;
			bidirectionalEdgeCount += bidirectionalEdges;

			triplet021dCount += unidirectionalEdgesAsSource * (unidirectionalEdgesAsSource - 1) / 2;
			triplet021uCount += unidirectionalEdgesAsTarget * (unidirectionalEdgesAsTarget - 1) / 2;
			triplet021cCount += unidirectionalEdgesAsSource * unidirectionalEdgesAsTarget;
			triplet111dCount += unidirectionalEdgesAsTarget * bidirectionalEdges;
			triplet111uCount += unidirectionalEdgesAsSource * bidirectionalEdges;
			triplet201Count += bidirectionalEdges * (bidirectionalEdges - 1) / 2;
		}

		@Override
		public void close() throws IOException {
			addAccumulator("vc", new LongCounter(vertexCount));
			addAccumulator("uec", new LongCounter(unidirectionalEdgeCount));
			addAccumulator("bec", new LongCounter(bidirectionalEdgeCount));
			addAccumulator("021d", new LongCounter(triplet021dCount));
			addAccumulator("021u", new LongCounter(triplet021uCount));
			addAccumulator("021c", new LongCounter(triplet021cCount));
			addAccumulator("111d", new LongCounter(triplet111dCount));
			addAccumulator("111u", new LongCounter(triplet111uCount));
			addAccumulator("201", new LongCounter(triplet201Count));
		}
	}

	/**
	 * Wraps triadic census metrics.
	 */
	public static class Result
	implements PrintableResult {
		private final BigInteger[] counts;

		public Result(BigInteger... counts) {
			Preconditions.checkArgument(counts.length == 16,
				"Expected 16 counts but received " + counts.length);

			this.counts = counts;
		}

		public Result(long... counts) {
			Preconditions.checkArgument(counts.length == 16,
				"Expected 16 counts but received " + counts.length);

			this.counts = new BigInteger[counts.length];

			for (int i = 0; i < counts.length; i++) {
				this.counts[i] = BigInteger.valueOf(counts[i]);
			}
		}

		/**
		 * Get the count of "003" triads which have zero connecting vertices.
		 *
		 * @return count of "003" triads
		 */
		public BigInteger getCount003() {
			return counts[0];
		}

		/**
		 * Get the count of "012" triads which have one unidirectional edge among the vertices.
		 *
		 * @return count of "012" triads
		 */
		public BigInteger getCount012() {
			return counts[1];
		}

		/**
		 * Get the count of "102" triads which have one bidirectional edge among the vertices.
		 *
		 * @return count of "102" triads
		 */
		public BigInteger getCount102() {
			return counts[2];
		}

		/**
		 * Get the count of "021d" triads which have two unidirectional edges among the vertices,
		 * forming an open triplet; both edges source the center vertex.
		 *
		 * @return count of "021d" triads
		 */
		public BigInteger getCount021d() {
			return counts[3];
		}

		/**
		 * Get the count of "021u" triads which have two unidirectional edges among the vertices,
		 * forming an open triplet; both edges target the center vertex.
		 *
		 * @return count of "021u" triads
		 */
		public BigInteger getCount021u() {
			return counts[4];
		}

		/**
		 * Get the count of "021c" triads which have two unidirectional edges among the vertices,
		 * forming an open triplet; one edge sources and one edge targets the center vertex.
		 *
		 * @return count of "021c" triads
		 */
		public BigInteger getCount021c() {
			return counts[5];
		}

		/**
		 * Get the count of "111d" triads which have one unidirectional and one bidirectional edge
		 * among the vertices, forming an open triplet; the unidirectional edge targets the center vertex.
		 *
		 * @return count of "111d" triads
		 */
		public BigInteger getCount111d() {
			return counts[6];
		}

		/**
		 * Get the count of "111u" triads which have one unidirectional and one bidirectional edge
		 * among the vertices, forming an open triplet; the unidirectional edge sources the center vertex.
		 *
		 * @return count of "111u" triads
		 */
		public BigInteger getCount111u() {
			return counts[7];
		}

		/**
		 * Get the count of "030t" triads which have three unidirectional edges among the vertices,
		 * forming a closed triplet, a triangle; two of the unidirectional edges source/target the
		 * same vertex.
		 *
		 * @return count of "030t" triads
		 */
		public BigInteger getCount030t() {
			return counts[8];
		}

		/**
		 * Get the count of "030c" triads which have three unidirectional edges among the vertices,
		 * forming a closed triplet, a triangle; the three unidirectional edges both source and target
		 * different vertices.
		 *
		 * @return count of "030c" triads
		 */
		public BigInteger getCount030c() {
			return counts[9];
		}

		/**
		 * Get the count of "201" triads which have two unidirectional edges among the vertices,
		 * forming an open triplet.
		 *
		 * @return count of "201" triads
		 */
		public BigInteger getCount201() {
			return counts[10];
		}

		/**
		 * Get the count of "120d" triads which have two unidirectional edges and one bidirectional edge
		 * among the vertices, forming a closed triplet, a triangle; both unidirectional edges source
		 * the same vertex.
		 *
		 * @return count of "120d" triads
		 */
		public BigInteger getCount120d() {
			return counts[11];
		}

		/**
		 * Get the count of "120u" triads which have two unidirectional and one bidirectional edges
		 * among the vertices, forming a closed triplet, a triangle; both unidirectional edges target
		 * the same vertex.
		 *
		 * @return count of "120u" triads
		 */
		public BigInteger getCount120u() {
			return counts[12];
		}

		/**
		 * Get the count of "120c" triads which have two unidirectional edges and one bidirectional edge
		 * among the vertices, forming a closed triplet, a triangle; one vertex is sourced by and targeted
		 * by the unidirectional edges.
		 *
		 * @return count of "120c" triads
		 */
		public BigInteger getCount120c() {
			return counts[13];
		}

		/**
		 * Get the count of "210" triads which have one unidirectional edge and two bidirectional edges
		 * among the vertices, forming a closed triplet, a triangle.
		 *
		 * @return count of "210" triads
		 */
		public BigInteger getCount210() {
			return counts[14];
		}

		/**
		 * Get the count of "300" triads which have three bidirectional edges among the vertices,
		 * forming a closed triplet, a triangle.
		 *
		 * @return count of "300" triads
		 */
		public BigInteger getCount300() {
			return counts[15];
		}

		/**
		 * Get the array of counts.
		 *
		 * <p>The order of the counts is from least to most connected:
		 *   003, 012, 102, 021d, 021u, 021c, 111d, 111u,
		 *   030t, 030c, 201, 120d, 120u, 120c, 210, 300
		 *
		 * @return array of counts
		 */
		public BigInteger[] getCounts() {
			return counts;
		}

		@Override
		public String toString() {
			return toPrintableString();
		}

		@Override
		public String toPrintableString() {
			NumberFormat nf = NumberFormat.getInstance();

			return "003: " + nf.format(getCount003())
				+ "; 012: " + nf.format(getCount012())
				+ "; 102: " + nf.format(getCount102())
				+ "; 021d: " + nf.format(getCount021d())
				+ "; 021u: " + nf.format(getCount021u())
				+ "; 021c: " + nf.format(getCount021c())
				+ "; 111d: " + nf.format(getCount111d())
				+ "; 111u: " + nf.format(getCount111u())
				+ "; 030t: " + nf.format(getCount030t())
				+ "; 030c: " + nf.format(getCount030c())
				+ "; 201: " + nf.format(getCount201())
				+ "; 120d: " + nf.format(getCount120d())
				+ "; 120u: " + nf.format(getCount120u())
				+ "; 120c: " + nf.format(getCount120c())
				+ "; 210: " + nf.format(getCount210())
				+ "; 300: " + nf.format(getCount300());
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder()
				.append(counts)
				.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}

			if (obj == this) {
				return true;
			}

			if (obj.getClass() != getClass()) {
				return false;
			}

			Result rhs = (Result) obj;

			return new EqualsBuilder()
				.append(counts, rhs.counts)
				.isEquals();
		}
	}
}
