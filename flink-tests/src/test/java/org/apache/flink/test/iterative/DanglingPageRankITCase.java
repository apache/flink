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

package org.apache.flink.test.iterative;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Implementation of PageRank accounting for "sink" vertices with 0 out-degree. */
@RunWith(Parameterized.class)
@SuppressWarnings({"serial", "unchecked"})
public class DanglingPageRankITCase extends MultipleProgramsTestBase {

    private static final String AGGREGATOR_NAME = "pagerank.aggregator";

    public DanglingPageRankITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Test
    public void testDanglingPageRank() {
        try {
            final int numIterations = 25;
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Tuple2<Long, Boolean>> vertices =
                    env.fromElements(
                            new Tuple2<>(1L, false),
                            new Tuple2<>(2L, false),
                            new Tuple2<>(5L, false),
                            new Tuple2<>(3L, true),
                            new Tuple2<>(4L, false));

            DataSet<PageWithLinks> edges =
                    env.fromElements(
                            new PageWithLinks(2L, new long[] {1}),
                            new PageWithLinks(5L, new long[] {2, 4}),
                            new PageWithLinks(4L, new long[] {3, 2}),
                            new PageWithLinks(1L, new long[] {4, 2, 3}));

            final long numVertices = vertices.count();
            final long numDanglingVertices =
                    vertices.filter(
                                    new FilterFunction<Tuple2<Long, Boolean>>() {
                                        @Override
                                        public boolean filter(Tuple2<Long, Boolean> value) {
                                            return value.f1;
                                        }
                                    })
                            .count();

            DataSet<PageWithRankAndDangling> verticesWithInitialRank =
                    vertices.map(
                            new MapFunction<Tuple2<Long, Boolean>, PageWithRankAndDangling>() {

                                @Override
                                public PageWithRankAndDangling map(Tuple2<Long, Boolean> value) {
                                    return new PageWithRankAndDangling(
                                            value.f0, 1.0 / numVertices, value.f1);
                                }
                            });

            IterativeDataSet<PageWithRankAndDangling> iteration =
                    verticesWithInitialRank.iterate(numIterations);

            iteration
                    .getAggregators()
                    .registerAggregationConvergenceCriterion(
                            AGGREGATOR_NAME,
                            new PageRankStatsAggregator(),
                            new DiffL1NormConvergenceCriterion());

            DataSet<PageWithRank> partialRanks =
                    iteration
                            .join(edges)
                            .where("pageId")
                            .equalTo("pageId")
                            .with(
                                    new FlatJoinFunction<
                                            PageWithRankAndDangling,
                                            PageWithLinks,
                                            PageWithRank>() {

                                        @Override
                                        public void join(
                                                PageWithRankAndDangling page,
                                                PageWithLinks links,
                                                Collector<PageWithRank> out) {

                                            double rankToDistribute =
                                                    page.rank / (double) links.targets.length;
                                            PageWithRank output =
                                                    new PageWithRank(0L, rankToDistribute);

                                            for (long target : links.targets) {
                                                output.pageId = target;
                                                out.collect(output);
                                            }
                                        }
                                    });

            DataSet<PageWithRankAndDangling> newRanks =
                    iteration
                            .coGroup(partialRanks)
                            .where("pageId")
                            .equalTo("pageId")
                            .with(
                                    new RichCoGroupFunction<
                                            PageWithRankAndDangling,
                                            PageWithRank,
                                            PageWithRankAndDangling>() {

                                        private static final double BETA = 0.85;

                                        private final double randomJump =
                                                (1.0 - BETA) / numVertices;
                                        private PageRankStatsAggregator aggregator;
                                        private double danglingRankFactor;

                                        @Override
                                        public void open(Configuration parameters)
                                                throws Exception {
                                            int currentIteration =
                                                    getIterationRuntimeContext()
                                                            .getSuperstepNumber();

                                            aggregator =
                                                    getIterationRuntimeContext()
                                                            .getIterationAggregator(
                                                                    AGGREGATOR_NAME);

                                            if (currentIteration == 1) {
                                                danglingRankFactor =
                                                        BETA
                                                                * (double) numDanglingVertices
                                                                / ((double) numVertices
                                                                        * (double) numVertices);
                                            } else {
                                                PageRankStats previousAggregate =
                                                        getIterationRuntimeContext()
                                                                .getPreviousIterationAggregate(
                                                                        AGGREGATOR_NAME);
                                                danglingRankFactor =
                                                        BETA
                                                                * previousAggregate.danglingRank()
                                                                / (double) numVertices;
                                            }
                                        }

                                        @Override
                                        public void coGroup(
                                                Iterable<PageWithRankAndDangling> currentPages,
                                                Iterable<PageWithRank> partialRanks,
                                                Collector<PageWithRankAndDangling> out) {

                                            // compute the next rank
                                            long edges = 0;
                                            double summedRank = 0;
                                            for (PageWithRank partial : partialRanks) {
                                                summedRank += partial.rank;
                                                edges++;
                                            }
                                            double rank =
                                                    BETA * summedRank
                                                            + randomJump
                                                            + danglingRankFactor;

                                            // current rank, for stats and convergence
                                            PageWithRankAndDangling currentPage =
                                                    currentPages.iterator().next();
                                            double currentRank = currentPage.rank;
                                            boolean isDangling = currentPage.dangling;

                                            // maintain statistics to compensate for probability
                                            // loss on dangling nodes
                                            double danglingRankToAggregate = isDangling ? rank : 0;
                                            long danglingVerticesToAggregate = isDangling ? 1 : 0;
                                            double diff = Math.abs(currentRank - rank);
                                            aggregator.aggregate(
                                                    diff,
                                                    rank,
                                                    danglingRankToAggregate,
                                                    danglingVerticesToAggregate,
                                                    1,
                                                    edges);

                                            currentPage.rank = rank;
                                            out.collect(currentPage);
                                        }
                                    });

            List<PageWithRankAndDangling> result = iteration.closeWith(newRanks).collect();

            double totalRank = 0.0;
            for (PageWithRankAndDangling r : result) {
                totalRank += r.rank;
                assertTrue(r.pageId >= 1 && r.pageId <= 5);
                assertTrue(r.pageId != 3 || r.dangling);
            }

            assertEquals(1.0, totalRank, 0.001);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // ------------------------------------------------------------------------
    //  custom types
    // ------------------------------------------------------------------------

    /** POJO for page ID and rank value. */
    public static class PageWithRank {

        public long pageId;
        public double rank;

        public PageWithRank() {}

        public PageWithRank(long pageId, double rank) {
            this.pageId = pageId;
            this.rank = rank;
        }
    }

    /** POJO for page ID, rank value, and whether a "dangling" vertex with 0 out-degree. */
    public static class PageWithRankAndDangling {

        public long pageId;
        public double rank;
        public boolean dangling;

        public PageWithRankAndDangling() {}

        public PageWithRankAndDangling(long pageId, double rank, boolean dangling) {
            this.pageId = pageId;
            this.rank = rank;
            this.dangling = dangling;
        }

        @Override
        public String toString() {
            return "PageWithRankAndDangling{"
                    + "pageId="
                    + pageId
                    + ", rank="
                    + rank
                    + ", dangling="
                    + dangling
                    + '}';
        }
    }

    /** POJO for page ID and list of target IDs. */
    public static class PageWithLinks {

        public long pageId;
        public long[] targets;

        public PageWithLinks() {}

        public PageWithLinks(long pageId, long[] targets) {
            this.pageId = pageId;
            this.targets = targets;
        }
    }

    // ------------------------------------------------------------------------
    //  statistics
    // ------------------------------------------------------------------------

    /** PageRank statistics. */
    public static class PageRankStats implements Value {

        private double diff;
        private double rank;
        private double danglingRank;
        private long numDanglingVertices;
        private long numVertices;
        private long edges;

        public PageRankStats() {}

        public PageRankStats(
                double diff,
                double rank,
                double danglingRank,
                long numDanglingVertices,
                long numVertices,
                long edges) {

            this.diff = diff;
            this.rank = rank;
            this.danglingRank = danglingRank;
            this.numDanglingVertices = numDanglingVertices;
            this.numVertices = numVertices;
            this.edges = edges;
        }

        public double diff() {
            return diff;
        }

        public double rank() {
            return rank;
        }

        public double danglingRank() {
            return danglingRank;
        }

        public long numDanglingVertices() {
            return numDanglingVertices;
        }

        public long numVertices() {
            return numVertices;
        }

        public long edges() {
            return edges;
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            out.writeDouble(diff);
            out.writeDouble(rank);
            out.writeDouble(danglingRank);
            out.writeLong(numDanglingVertices);
            out.writeLong(numVertices);
            out.writeLong(edges);
        }

        @Override
        public void read(DataInputView in) throws IOException {
            diff = in.readDouble();
            rank = in.readDouble();
            danglingRank = in.readDouble();
            numDanglingVertices = in.readLong();
            numVertices = in.readLong();
            edges = in.readLong();
        }

        @Override
        public String toString() {
            return "PageRankStats: diff ["
                    + diff
                    + "], rank ["
                    + rank
                    + "], danglingRank ["
                    + danglingRank
                    + "], numDanglingVertices ["
                    + numDanglingVertices
                    + "], numVertices ["
                    + numVertices
                    + "], edges ["
                    + edges
                    + "]";
        }
    }

    private static class PageRankStatsAggregator implements Aggregator<PageRankStats> {

        private double diff;
        private double rank;
        private double danglingRank;
        private long numDanglingVertices;
        private long numVertices;
        private long edges;

        @Override
        public PageRankStats getAggregate() {
            return new PageRankStats(
                    diff, rank, danglingRank, numDanglingVertices, numVertices, edges);
        }

        public void aggregate(
                double diffDelta,
                double rankDelta,
                double danglingRankDelta,
                long danglingVerticesDelta,
                long verticesDelta,
                long edgesDelta) {
            diff += diffDelta;
            rank += rankDelta;
            danglingRank += danglingRankDelta;
            numDanglingVertices += danglingVerticesDelta;
            numVertices += verticesDelta;
            edges += edgesDelta;
        }

        @Override
        public void aggregate(PageRankStats pageRankStats) {
            diff += pageRankStats.diff();
            rank += pageRankStats.rank();
            danglingRank += pageRankStats.danglingRank();
            numDanglingVertices += pageRankStats.numDanglingVertices();
            numVertices += pageRankStats.numVertices();
            edges += pageRankStats.edges();
        }

        @Override
        public void reset() {
            diff = 0;
            rank = 0;
            danglingRank = 0;
            numDanglingVertices = 0;
            numVertices = 0;
            edges = 0;
        }
    }

    private static class DiffL1NormConvergenceCriterion
            implements ConvergenceCriterion<PageRankStats> {

        private static final double EPSILON = 0.00005;

        @Override
        public boolean isConverged(int iteration, PageRankStats pageRankStats) {
            return pageRankStats.diff() < EPSILON;
        }
    }
}
