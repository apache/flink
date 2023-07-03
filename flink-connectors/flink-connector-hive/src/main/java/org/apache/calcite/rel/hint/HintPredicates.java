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

package org.apache.calcite.rel.hint;

/**
 * Copied from Flink code base.
 *
 * <p>A collection of hint predicates.
 *
 * <p>Temporarily copy from calcite to cherry-pick [CALCITE-5107] and will be removed when upgrade
 * the latest calcite.
 *
 * <p>Note: We need to copy to here from flink-table-planner for flink will use the HintPredicates
 * to build HintStrategyTable. If we don't copy HintPredicates to here,it will use Calcite's
 * HintPredicates to build HintStrategyTable with Hive dialect, which will then bring unexpected
 * exception. Also can be removed when upgrade the latest calcite.
 */
public abstract class HintPredicates {
    /**
     * A hint predicate that indicates a hint can only be used to the whole query(no specific
     * nodes).
     */
    public static final HintPredicate SET_VAR =
            new NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.SET_VAR);

    /**
     * A hint predicate that indicates a hint can only be used to {@link
     * org.apache.calcite.rel.core.Join} nodes.
     */
    public static final HintPredicate JOIN =
            new NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.JOIN);

    /**
     * A hint predicate that indicates a hint can only be used to {@link
     * org.apache.calcite.rel.core.TableScan} nodes.
     */
    public static final HintPredicate TABLE_SCAN =
            new NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.TABLE_SCAN);

    /**
     * A hint predicate that indicates a hint can only be used to {@link
     * org.apache.calcite.rel.core.Filter} nodes.
     */
    public static final HintPredicate FILTER =
            new NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.FILTER);

    /**
     * A hint predicate that indicates a hint can only be used to {@link
     * org.apache.calcite.rel.core.Project} nodes.
     */
    public static final HintPredicate PROJECT =
            new NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.PROJECT);

    /**
     * A hint predicate that indicates a hint can only be used to {@link
     * org.apache.calcite.rel.core.Correlate} nodes.
     */
    public static final HintPredicate CORRELATE =
            new NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.CORRELATE);

    /**
     * A hint predicate that indicates a hint can only be used to {@link
     * org.apache.calcite.rel.core.SetOp} nodes.
     */
    public static final HintPredicate SETOP =
            new NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.SETOP);

    /**
     * A hint predicate that indicates a hint can only be used to {@link
     * org.apache.calcite.rel.core.Sort} nodes.
     */
    public static final HintPredicate SORT =
            new NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.SORT);

    /**
     * A hint predicate that indicates a hint can only be used to {@link
     * org.apache.calcite.rel.core.Aggregate} nodes.
     */
    public static final HintPredicate AGGREGATE =
            new NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.AGGREGATE);

    /**
     * A hint predicate that indicates a hint can only be used to {@link
     * org.apache.calcite.rel.core.Calc} nodes.
     */
    public static final HintPredicate CALC =
            new NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.CALC);

    /**
     * A hint predicate that indicates a hint can only be used to {@link
     * org.apache.calcite.rel.core.Values} nodes.
     */
    public static final HintPredicate VALUES =
            new NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.VALUES);

    /**
     * A hint predicate that indicates a hint can only be used to {@link
     * org.apache.calcite.rel.core.Window} nodes.
     */
    public static final HintPredicate WINDOW =
            new NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.WINDOW);

    /**
     * A hint predicate that indicates a hint can only be used to {@link
     * org.apache.calcite.rel.core.Snapshot} nodes.
     */
    public static final HintPredicate SNAPSHOT =
            new NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.SNAPSHOT);

    /**
     * Returns a composed hint predicate that represents a short-circuiting logical AND of an array
     * of hint predicates {@code hintPredicates}. When evaluating the composed predicate, if a
     * predicate is {@code false}, then all the left predicates are not evaluated.
     *
     * <p>The predicates are evaluated in sequence.
     */
    public static HintPredicate and(HintPredicate... hintPredicates) {
        return new CompositeHintPredicate(CompositeHintPredicate.Composition.AND, hintPredicates);
    }

    /**
     * Returns a composed hint predicate that represents a short-circuiting logical OR of an array
     * of hint predicates {@code hintPredicates}. When evaluating the composed predicate, if a
     * predicate is {@code true}, then all the left predicates are not evaluated.
     *
     * <p>The predicates are evaluated in sequence.
     */
    public static HintPredicate or(HintPredicate... hintPredicates) {
        return new CompositeHintPredicate(CompositeHintPredicate.Composition.OR, hintPredicates);
    }
}
