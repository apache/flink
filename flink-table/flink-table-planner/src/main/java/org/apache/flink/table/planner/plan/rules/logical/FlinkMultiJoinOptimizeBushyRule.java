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

package org.apache.flink.table.planner.plan.rules.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class FlinkMultiJoinOptimizeBushyRule extends RelRule<FlinkMultiJoinOptimizeBushyRule.Config>
        implements TransformationRule {

    public static final FlinkMultiJoinOptimizeBushyRule INSTANCE =
            FlinkMultiJoinOptimizeBushyRule.Config.DEFAULT.toRule();

    protected FlinkMultiJoinOptimizeBushyRule(FlinkMultiJoinOptimizeBushyRule.Config config) {
        super(config);
    }

    private final @Nullable PrintWriter pw = CalciteSystemProperty.DEBUG.value()
            ? Util.printWriter(System.out)
            : null;

    @Override public void onMatch(RelOptRuleCall call) {
        final MultiJoin multiJoinRel = call.rel(0);
        final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
        final RelBuilder relBuilder = call.builder();
        final RelMetadataQuery mq = call.getMetadataQuery();

        final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);

        final List<FlinkMultiJoinOptimizeBushyRule.Vertex> vertexes = new ArrayList<>();
        int x = 0;
        for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
            final RelNode rel = multiJoin.getJoinFactor(i);
            double cost = mq.getRowCount(rel);
            vertexes.add(new FlinkMultiJoinOptimizeBushyRule.LeafVertex(i, rel, cost, x));
            x += rel.getRowType().getFieldCount();
        }
        assert x == multiJoin.getNumTotalFields();

        final List<LoptMultiJoin.Edge> unusedEdges = new ArrayList<>();
        for (RexNode node : multiJoin.getJoinFilters()) {
            unusedEdges.add(multiJoin.createEdge(node));
        }

        // Comparator that chooses the best edge. A "good edge" is one that has
        // a large difference in the number of rows on LHS and RHS.
        final Comparator<LoptMultiJoin.Edge> edgeComparator =
                new Comparator<LoptMultiJoin.Edge>() {
                    @Override public int compare(LoptMultiJoin.Edge e0, LoptMultiJoin.Edge e1) {
                        return Double.compare(rowCountDiff(e0), rowCountDiff(e1));
                    }

                    private double rowCountDiff(LoptMultiJoin.Edge edge) {
                        assert edge.factors.cardinality() == 2 : edge.factors;
                        final int factor0 = edge.factors.nextSetBit(0);
                        final int factor1 = edge.factors.nextSetBit(factor0 + 1);
                        return Math.abs(vertexes.get(factor0).cost
                                - vertexes.get(factor1).cost);
                    }
                };

        final List<LoptMultiJoin.Edge> usedEdges = new ArrayList<>();
        for (;;) {
            final int edgeOrdinal = chooseBestEdge(unusedEdges, edgeComparator);
            if (pw != null) {
                trace(vertexes, unusedEdges, usedEdges, edgeOrdinal, pw);
            }
            final int[] factors;
            if (edgeOrdinal == -1) {
                // No more edges. Are there any un-joined vertexes?
                final FlinkMultiJoinOptimizeBushyRule.Vertex lastVertex = Util.last(vertexes);
                final int z = lastVertex.factors.previousClearBit(lastVertex.id - 1);
                if (z < 0) {
                    break;
                }
                factors = new int[] {z, lastVertex.id};
            } else {
                final LoptMultiJoin.Edge bestEdge = unusedEdges.get(edgeOrdinal);

                // For now, assume that the edge is between precisely two factors.
                // 1-factor conditions have probably been pushed down,
                // and 3-or-more-factor conditions are advanced. (TODO:)
                // Therefore, for now, the factors that are merged are exactly the
                // factors on this edge.
                assert bestEdge.factors.cardinality() == 2;
                factors = bestEdge.factors.toArray();
            }

            // Determine which factor is to be on the LHS of the join.
            final int majorFactor;
            final int minorFactor;
            if (vertexes.get(factors[0]).cost <= vertexes.get(factors[1]).cost) {
                majorFactor = factors[0];
                minorFactor = factors[1];
            } else {
                majorFactor = factors[1];
                minorFactor = factors[0];
            }
            final FlinkMultiJoinOptimizeBushyRule.Vertex majorVertex = vertexes.get(majorFactor);
            final FlinkMultiJoinOptimizeBushyRule.Vertex minorVertex = vertexes.get(minorFactor);

            // Find the join conditions. All conditions whose factors are now all in
            // the join can now be used.
            final int v = vertexes.size();
            final ImmutableBitSet newFactors =
                    majorVertex.factors
                            .rebuild()
                            .addAll(minorVertex.factors)
                            .set(v)
                            .build();

            final List<RexNode> conditions = new ArrayList<>();
            final Iterator<LoptMultiJoin.Edge> edgeIterator = unusedEdges.iterator();
            while (edgeIterator.hasNext()) {
                LoptMultiJoin.Edge edge = edgeIterator.next();
                if (newFactors.contains(edge.factors)) {
                    conditions.add(edge.condition);
                    edgeIterator.remove();
                    usedEdges.add(edge);
                }
            }

            double cost =
                    majorVertex.cost
                            * minorVertex.cost
                            * RelMdUtil.guessSelectivity(
                            RexUtil.composeConjunction(rexBuilder, conditions));
            final FlinkMultiJoinOptimizeBushyRule.Vertex newVertex =
                    new FlinkMultiJoinOptimizeBushyRule.JoinVertex(v, majorFactor, minorFactor, newFactors,
                            cost, ImmutableList.copyOf(conditions));
            vertexes.add(newVertex);

            // Re-compute selectivity of edges above the one just chosen.
            // Suppose that we just chose the edge between "product" (10k rows) and
            // "product_class" (10 rows).
            // Both of those vertices are now replaced by a new vertex "P-PC".
            // This vertex has fewer rows (1k rows) -- a fact that is critical to
            // decisions made later. (Hence "greedy" algorithm not "simple".)
            // The adjacent edges are modified.
            final ImmutableBitSet merged =
                    ImmutableBitSet.of(minorFactor, majorFactor);
            for (int i = 0; i < unusedEdges.size(); i++) {
                final LoptMultiJoin.Edge edge = unusedEdges.get(i);
                if (edge.factors.intersects(merged)) {
                    ImmutableBitSet newEdgeFactors =
                            edge.factors
                                    .rebuild()
                                    .removeAll(newFactors)
                                    .set(v)
                                    .build();
                    assert newEdgeFactors.cardinality() == 2;
                    final LoptMultiJoin.Edge newEdge =
                            new LoptMultiJoin.Edge(edge.condition, newEdgeFactors,
                                    edge.columns);
                    unusedEdges.set(i, newEdge);
                }
            }
        }

        // We have a winner!
        List<Pair<RelNode, Mappings.TargetMapping>> relNodes = new ArrayList<>();
        for (FlinkMultiJoinOptimizeBushyRule.Vertex vertex : vertexes) {
            if (vertex instanceof FlinkMultiJoinOptimizeBushyRule.LeafVertex) {
                FlinkMultiJoinOptimizeBushyRule.LeafVertex leafVertex = (FlinkMultiJoinOptimizeBushyRule.LeafVertex) vertex;
                final Mappings.TargetMapping mapping =
                        Mappings.offsetSource(
                                Mappings.createIdentity(
                                        leafVertex.rel.getRowType().getFieldCount()),
                                leafVertex.fieldOffset,
                                multiJoin.getNumTotalFields());
                relNodes.add(Pair.of(leafVertex.rel, mapping));
            } else {
                FlinkMultiJoinOptimizeBushyRule.JoinVertex joinVertex = (FlinkMultiJoinOptimizeBushyRule.JoinVertex) vertex;
                final Pair<RelNode, Mappings.TargetMapping> leftPair =
                        relNodes.get(joinVertex.leftFactor);
                RelNode left = leftPair.left;
                final Mappings.TargetMapping leftMapping = leftPair.right;
                final Pair<RelNode, Mappings.TargetMapping> rightPair =
                        relNodes.get(joinVertex.rightFactor);
                RelNode right = rightPair.left;
                final Mappings.TargetMapping rightMapping = rightPair.right;
                final Mappings.TargetMapping mapping =
                        Mappings.merge(leftMapping,
                                Mappings.offsetTarget(rightMapping,
                                        left.getRowType().getFieldCount()));
                if (pw != null) {
                    pw.println("left: " + leftMapping);
                    pw.println("right: " + rightMapping);
                    pw.println("combined: " + mapping);
                    pw.println();
                }
                final RexVisitor<RexNode> shuttle =
                        new RexPermuteInputsShuttle(mapping, left, right);
                final RexNode condition =
                        RexUtil.composeConjunction(rexBuilder, joinVertex.conditions);

                final RelNode join = relBuilder.push(left)
                        .push(right)
                        .join(JoinRelType.INNER, condition.accept(shuttle))
                        .build();
                relNodes.add(Pair.of(join, mapping));
            }
            if (pw != null) {
                pw.println(Util.last(relNodes));
            }
        }

        final Pair<RelNode, Mappings.TargetMapping> top = Util.last(relNodes);
        relBuilder.push(top.left)
                .project(relBuilder.fields(top.right));
        call.transformTo(relBuilder.build());
    }

    private static void trace(List<FlinkMultiJoinOptimizeBushyRule.Vertex> vertexes,
                              List<LoptMultiJoin.Edge> unusedEdges, List<LoptMultiJoin.Edge> usedEdges,
                              int edgeOrdinal, PrintWriter pw) {
        pw.println("bestEdge: " + edgeOrdinal);
        pw.println("vertexes:");
        for (FlinkMultiJoinOptimizeBushyRule.Vertex vertex : vertexes) {
            pw.println(vertex);
        }
        pw.println("unused edges:");
        for (LoptMultiJoin.Edge edge : unusedEdges) {
            pw.println(edge);
        }
        pw.println("edges:");
        for (LoptMultiJoin.Edge edge : usedEdges) {
            pw.println(edge);
        }
        pw.println();
        pw.flush();
    }

    int chooseBestEdge(List<LoptMultiJoin.Edge> edges,
                       Comparator<LoptMultiJoin.Edge> comparator) {
        return minPos(edges, comparator);
    }

    /** Returns the index within a list at which compares least according to a
     * comparator.
     *
     * <p>In the case of a tie, returns the earliest such element.</p>
     *
     * <p>If the list is empty, returns -1.</p>
     */
    static <E> int minPos(List<E> list, Comparator<E> fn) {
        if (list.isEmpty()) {
            return -1;
        }
        E eBest = list.get(0);
        int iBest = 0;
        for (int i = 1; i < list.size(); i++) {
            E e = list.get(i);
            if (fn.compare(e, eBest) < 0) {
                eBest = e;
                iBest = i;
            }
        }
        return iBest;
    }

    /** Participant in a join (relation or join). */
    abstract static class Vertex {
        final int id;

        protected final ImmutableBitSet factors;
        final double cost;

        Vertex(int id, ImmutableBitSet factors, double cost) {
            this.id = id;
            this.factors = factors;
            this.cost = cost;
        }
    }

    /** Relation participating in a join. */
    static class LeafVertex extends Vertex {
        private final RelNode rel;
        final int fieldOffset;

        LeafVertex(int id, RelNode rel, double cost, int fieldOffset) {
            super(id, ImmutableBitSet.of(id), cost);
            this.rel = rel;
            this.fieldOffset = fieldOffset;
        }

        @Override public String toString() {
            return "LeafVertex(id: " + id
                    + ", cost: " + Util.human(cost)
                    + ", factors: " + factors
                    + ", fieldOffset: " + fieldOffset
                    + ")";
        }
    }

    /** Participant in a join which is itself a join. */
    static class JoinVertex extends Vertex {
        private final int leftFactor;
        private final int rightFactor;
        /** Zero or more join conditions. All are in terms of the original input
         * columns (not in terms of the outputs of left and right input factors). */
        final ImmutableList<RexNode> conditions;

        JoinVertex(int id, int leftFactor, int rightFactor, ImmutableBitSet factors,
                   double cost, ImmutableList<RexNode> conditions) {
            super(id, factors, cost);
            this.leftFactor = leftFactor;
            this.rightFactor = rightFactor;
            this.conditions = Objects.requireNonNull(conditions, "conditions");
        }

        @Override public String toString() {
            return "JoinVertex(id: " + id
                    + ", cost: " + Util.human(cost)
                    + ", factors: " + factors
                    + ", leftFactor: " + leftFactor
                    + ", rightFactor: " + rightFactor
                    + ")";
        }
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        FlinkMultiJoinOptimizeBushyRule.Config DEFAULT = ImmutableFlinkMultiJoinOptimizeBushyRule.Config.of()
                .withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs());

        @Override default FlinkMultiJoinOptimizeBushyRule toRule() {
            return new FlinkMultiJoinOptimizeBushyRule(this);
        }
    }
}
