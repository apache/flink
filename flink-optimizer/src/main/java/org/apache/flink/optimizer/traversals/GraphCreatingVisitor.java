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

package org.apache.flink.optimizer.traversals;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Union;
import org.apache.flink.api.common.operators.base.BulkIterationBase;
import org.apache.flink.api.common.operators.base.CoGroupOperatorBase;
import org.apache.flink.api.common.operators.base.CoGroupRawOperatorBase;
import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.common.operators.base.DeltaIterationBase;
import org.apache.flink.api.common.operators.base.FilterOperatorBase;
import org.apache.flink.api.common.operators.base.FlatMapOperatorBase;
import org.apache.flink.api.common.operators.base.GroupCombineOperatorBase;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.base.InnerJoinOperatorBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.base.MapPartitionOperatorBase;
import org.apache.flink.api.common.operators.base.OuterJoinOperatorBase;
import org.apache.flink.api.common.operators.base.PartitionOperatorBase;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.common.operators.base.SortPartitionOperatorBase;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.dag.BinaryUnionNode;
import org.apache.flink.optimizer.dag.BulkIterationNode;
import org.apache.flink.optimizer.dag.BulkPartialSolutionNode;
import org.apache.flink.optimizer.dag.CoGroupNode;
import org.apache.flink.optimizer.dag.CoGroupRawNode;
import org.apache.flink.optimizer.dag.CrossNode;
import org.apache.flink.optimizer.dag.DagConnection;
import org.apache.flink.optimizer.dag.DataSinkNode;
import org.apache.flink.optimizer.dag.DataSourceNode;
import org.apache.flink.optimizer.dag.FilterNode;
import org.apache.flink.optimizer.dag.FlatMapNode;
import org.apache.flink.optimizer.dag.GroupCombineNode;
import org.apache.flink.optimizer.dag.GroupReduceNode;
import org.apache.flink.optimizer.dag.JoinNode;
import org.apache.flink.optimizer.dag.MapNode;
import org.apache.flink.optimizer.dag.MapPartitionNode;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.dag.OuterJoinNode;
import org.apache.flink.optimizer.dag.PartitionNode;
import org.apache.flink.optimizer.dag.ReduceNode;
import org.apache.flink.optimizer.dag.SolutionSetNode;
import org.apache.flink.optimizer.dag.SortPartitionNode;
import org.apache.flink.optimizer.dag.WorksetIterationNode;
import org.apache.flink.optimizer.dag.WorksetNode;
import org.apache.flink.util.Visitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This traversal creates the optimizer DAG from a program. It works as a visitor that walks the
 * program's flow in a depth-first fashion, starting from the data sinks. During the descent it
 * creates an optimizer node for each operator, respectively data source or sink. During the ascent
 * it connects the nodes to the full graph.
 */
public class GraphCreatingVisitor implements Visitor<Operator<?>> {

    private final Map<Operator<?>, OptimizerNode>
            con2node; // map from the operator objects to their
    // corresponding optimizer nodes

    private final List<DataSinkNode> sinks; // all data sink nodes in the optimizer plan

    private final int defaultParallelism; // the default parallelism

    private final GraphCreatingVisitor
            parent; // reference to enclosing creator, in case of a recursive translation

    private final ExecutionMode defaultDataExchangeMode;

    private final boolean forceParallelism;

    public GraphCreatingVisitor(int defaultParallelism, ExecutionMode defaultDataExchangeMode) {
        this(null, false, defaultParallelism, defaultDataExchangeMode, null);
    }

    private GraphCreatingVisitor(
            GraphCreatingVisitor parent,
            boolean forceParallelism,
            int defaultParallelism,
            ExecutionMode dataExchangeMode,
            HashMap<Operator<?>, OptimizerNode> closure) {
        if (closure == null) {
            con2node = new HashMap<Operator<?>, OptimizerNode>();
        } else {
            con2node = closure;
        }

        this.sinks = new ArrayList<DataSinkNode>(2);
        this.defaultParallelism = defaultParallelism;
        this.parent = parent;
        this.defaultDataExchangeMode = dataExchangeMode;
        this.forceParallelism = forceParallelism;
    }

    public List<DataSinkNode> getSinks() {
        return sinks;
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean preVisit(Operator<?> c) {
        // check if we have been here before
        if (this.con2node.containsKey(c)) {
            return false;
        }

        final OptimizerNode n;

        // create a node for the operator (or sink or source) if we have not been here before
        if (c instanceof GenericDataSinkBase) {
            DataSinkNode dsn = new DataSinkNode((GenericDataSinkBase<?>) c);
            this.sinks.add(dsn);
            n = dsn;
        } else if (c instanceof GenericDataSourceBase) {
            n = new DataSourceNode((GenericDataSourceBase<?, ?>) c);
        } else if (c instanceof MapOperatorBase) {
            n = new MapNode((MapOperatorBase<?, ?, ?>) c);
        } else if (c instanceof MapPartitionOperatorBase) {
            n = new MapPartitionNode((MapPartitionOperatorBase<?, ?, ?>) c);
        } else if (c instanceof FlatMapOperatorBase) {
            n = new FlatMapNode((FlatMapOperatorBase<?, ?, ?>) c);
        } else if (c instanceof FilterOperatorBase) {
            n = new FilterNode((FilterOperatorBase<?, ?>) c);
        } else if (c instanceof ReduceOperatorBase) {
            n = new ReduceNode((ReduceOperatorBase<?, ?>) c);
        } else if (c instanceof GroupCombineOperatorBase) {
            n = new GroupCombineNode((GroupCombineOperatorBase<?, ?, ?>) c);
        } else if (c instanceof GroupReduceOperatorBase) {
            n = new GroupReduceNode((GroupReduceOperatorBase<?, ?, ?>) c);
        } else if (c instanceof InnerJoinOperatorBase) {
            n = new JoinNode((InnerJoinOperatorBase<?, ?, ?, ?>) c);
        } else if (c instanceof OuterJoinOperatorBase) {
            n = new OuterJoinNode((OuterJoinOperatorBase<?, ?, ?, ?>) c);
        } else if (c instanceof CoGroupOperatorBase) {
            n = new CoGroupNode((CoGroupOperatorBase<?, ?, ?, ?>) c);
        } else if (c instanceof CoGroupRawOperatorBase) {
            n = new CoGroupRawNode((CoGroupRawOperatorBase<?, ?, ?, ?>) c);
        } else if (c instanceof CrossOperatorBase) {
            n = new CrossNode((CrossOperatorBase<?, ?, ?, ?>) c);
        } else if (c instanceof BulkIterationBase) {
            n = new BulkIterationNode((BulkIterationBase<?>) c);
        } else if (c instanceof DeltaIterationBase) {
            n = new WorksetIterationNode((DeltaIterationBase<?, ?>) c);
        } else if (c instanceof Union) {
            n = new BinaryUnionNode((Union<?>) c);
        } else if (c instanceof PartitionOperatorBase) {
            n = new PartitionNode((PartitionOperatorBase<?>) c);
        } else if (c instanceof SortPartitionOperatorBase) {
            n = new SortPartitionNode((SortPartitionOperatorBase<?>) c);
        } else if (c instanceof BulkIterationBase.PartialSolutionPlaceHolder) {
            if (this.parent == null) {
                throw new InvalidProgramException(
                        "It is currently not supported to create data sinks inside iterations.");
            }

            final BulkIterationBase.PartialSolutionPlaceHolder<?> holder =
                    (BulkIterationBase.PartialSolutionPlaceHolder<?>) c;
            final BulkIterationBase<?> enclosingIteration = holder.getContainingBulkIteration();
            final BulkIterationNode containingIterationNode =
                    (BulkIterationNode) this.parent.con2node.get(enclosingIteration);

            // catch this for the recursive translation of step functions
            BulkPartialSolutionNode p =
                    new BulkPartialSolutionNode(holder, containingIterationNode);
            p.setParallelism(containingIterationNode.getParallelism());
            n = p;
        } else if (c instanceof DeltaIterationBase.WorksetPlaceHolder) {
            if (this.parent == null) {
                throw new InvalidProgramException(
                        "It is currently not supported to create data sinks inside iterations.");
            }

            final DeltaIterationBase.WorksetPlaceHolder<?> holder =
                    (DeltaIterationBase.WorksetPlaceHolder<?>) c;
            final DeltaIterationBase<?, ?> enclosingIteration =
                    holder.getContainingWorksetIteration();
            final WorksetIterationNode containingIterationNode =
                    (WorksetIterationNode) this.parent.con2node.get(enclosingIteration);

            // catch this for the recursive translation of step functions
            WorksetNode p = new WorksetNode(holder, containingIterationNode);
            p.setParallelism(containingIterationNode.getParallelism());
            n = p;
        } else if (c instanceof DeltaIterationBase.SolutionSetPlaceHolder) {
            if (this.parent == null) {
                throw new InvalidProgramException(
                        "It is currently not supported to create data sinks inside iterations.");
            }

            final DeltaIterationBase.SolutionSetPlaceHolder<?> holder =
                    (DeltaIterationBase.SolutionSetPlaceHolder<?>) c;
            final DeltaIterationBase<?, ?> enclosingIteration =
                    holder.getContainingWorksetIteration();
            final WorksetIterationNode containingIterationNode =
                    (WorksetIterationNode) this.parent.con2node.get(enclosingIteration);

            // catch this for the recursive translation of step functions
            SolutionSetNode p = new SolutionSetNode(holder, containingIterationNode);
            p.setParallelism(containingIterationNode.getParallelism());
            n = p;
        } else {
            throw new IllegalArgumentException("Unknown operator type: " + c);
        }

        this.con2node.put(c, n);

        // set the parallelism only if it has not been set before. some nodes have a fixed
        // parallelism, such as the
        // key-less reducer (all-reduce)
        if (n.getParallelism() < 1) {
            // set the parallelism
            int par = c.getParallelism();
            if (n instanceof BinaryUnionNode) {
                // Keep parallelism of union undefined for now.
                // It will be determined based on the parallelism of its successor.
                par = -1;
            } else if (par > 0) {
                if (this.forceParallelism && par != this.defaultParallelism) {
                    par = this.defaultParallelism;
                    Optimizer.LOG.warn(
                            "The parallelism of nested dataflows (such as step functions in iterations) is "
                                    + "currently fixed to the parallelism of the surrounding operator (the iteration).");
                }
            } else {
                par = this.defaultParallelism;
            }
            n.setParallelism(par);
        }

        return true;
    }

    @Override
    public void postVisit(Operator<?> c) {

        OptimizerNode n = this.con2node.get(c);

        // first connect to the predecessors
        n.setInput(this.con2node, this.defaultDataExchangeMode);
        n.setBroadcastInputs(this.con2node, this.defaultDataExchangeMode);

        // if the node represents a bulk iteration, we recursively translate the data flow now
        if (n instanceof BulkIterationNode) {
            final BulkIterationNode iterNode = (BulkIterationNode) n;
            final BulkIterationBase<?> iter = iterNode.getIterationContract();

            // pass a copy of the no iterative part into the iteration translation,
            // in case the iteration references its closure
            HashMap<Operator<?>, OptimizerNode> closure =
                    new HashMap<Operator<?>, OptimizerNode>(con2node);

            // first, recursively build the data flow for the step function
            final GraphCreatingVisitor recursiveCreator =
                    new GraphCreatingVisitor(
                            this,
                            true,
                            iterNode.getParallelism(),
                            defaultDataExchangeMode,
                            closure);

            BulkPartialSolutionNode partialSolution;

            iter.getNextPartialSolution().accept(recursiveCreator);

            partialSolution =
                    (BulkPartialSolutionNode)
                            recursiveCreator.con2node.get(iter.getPartialSolution());
            OptimizerNode rootOfStepFunction =
                    recursiveCreator.con2node.get(iter.getNextPartialSolution());
            if (partialSolution == null) {
                throw new CompilerException(
                        "Error: The step functions result does not depend on the partial solution.");
            }

            OptimizerNode terminationCriterion = null;

            if (iter.getTerminationCriterion() != null) {
                terminationCriterion =
                        recursiveCreator.con2node.get(iter.getTerminationCriterion());

                // no intermediate node yet, traverse from the termination criterion to build the
                // missing parts
                if (terminationCriterion == null) {
                    iter.getTerminationCriterion().accept(recursiveCreator);
                    terminationCriterion =
                            recursiveCreator.con2node.get(iter.getTerminationCriterion());
                }
            }

            iterNode.setPartialSolution(partialSolution);
            iterNode.setNextPartialSolution(rootOfStepFunction, terminationCriterion);

            // go over the contained data flow and mark the dynamic path nodes
            StaticDynamicPathIdentifier identifier =
                    new StaticDynamicPathIdentifier(iterNode.getCostWeight());
            iterNode.acceptForStepFunction(identifier);
        } else if (n instanceof WorksetIterationNode) {
            final WorksetIterationNode iterNode = (WorksetIterationNode) n;
            final DeltaIterationBase<?, ?> iter = iterNode.getIterationContract();

            // we need to ensure that both the next-workset and the solution-set-delta depend on the
            // workset.
            // One check is for free during the translation, we do the other check here as a
            // pre-condition
            {
                StepFunctionValidator wsf = new StepFunctionValidator();
                iter.getNextWorkset().accept(wsf);
                if (!wsf.hasFoundWorkset()) {
                    throw new CompilerException(
                            "In the given program, the next workset does not depend on the workset. "
                                    + "This is a prerequisite in delta iterations.");
                }
            }

            // calculate the closure of the anonymous function
            HashMap<Operator<?>, OptimizerNode> closure =
                    new HashMap<Operator<?>, OptimizerNode>(con2node);

            // first, recursively build the data flow for the step function
            final GraphCreatingVisitor recursiveCreator =
                    new GraphCreatingVisitor(
                            this,
                            true,
                            iterNode.getParallelism(),
                            defaultDataExchangeMode,
                            closure);

            // descend from the solution set delta. check that it depends on both the workset
            // and the solution set. If it does depend on both, this descend should create both
            // nodes
            iter.getSolutionSetDelta().accept(recursiveCreator);

            final WorksetNode worksetNode =
                    (WorksetNode) recursiveCreator.con2node.get(iter.getWorkset());

            if (worksetNode == null) {
                throw new CompilerException(
                        "In the given program, the solution set delta does not depend on the workset."
                                + "This is a prerequisite in delta iterations.");
            }

            iter.getNextWorkset().accept(recursiveCreator);

            SolutionSetNode solutionSetNode =
                    (SolutionSetNode) recursiveCreator.con2node.get(iter.getSolutionSet());

            if (solutionSetNode == null
                    || solutionSetNode.getOutgoingConnections() == null
                    || solutionSetNode.getOutgoingConnections().isEmpty()) {
                solutionSetNode =
                        new SolutionSetNode(
                                (DeltaIterationBase.SolutionSetPlaceHolder<?>)
                                        iter.getSolutionSet(),
                                iterNode);
            } else {
                for (DagConnection conn : solutionSetNode.getOutgoingConnections()) {
                    OptimizerNode successor = conn.getTarget();

                    if (successor.getClass() == JoinNode.class) {
                        // find out which input to the match the solution set is
                        JoinNode mn = (JoinNode) successor;
                        if (mn.getFirstPredecessorNode() == solutionSetNode) {
                            mn.makeJoinWithSolutionSet(0);
                        } else if (mn.getSecondPredecessorNode() == solutionSetNode) {
                            mn.makeJoinWithSolutionSet(1);
                        } else {
                            throw new CompilerException();
                        }
                    } else if (successor.getClass() == CoGroupNode.class) {
                        CoGroupNode cg = (CoGroupNode) successor;
                        if (cg.getFirstPredecessorNode() == solutionSetNode) {
                            cg.makeCoGroupWithSolutionSet(0);
                        } else if (cg.getSecondPredecessorNode() == solutionSetNode) {
                            cg.makeCoGroupWithSolutionSet(1);
                        } else {
                            throw new CompilerException();
                        }
                    } else {
                        throw new InvalidProgramException(
                                "Error: The only operations allowed on the solution set are Join and CoGroup.");
                    }
                }
            }

            final OptimizerNode nextWorksetNode =
                    recursiveCreator.con2node.get(iter.getNextWorkset());
            final OptimizerNode solutionSetDeltaNode =
                    recursiveCreator.con2node.get(iter.getSolutionSetDelta());

            // set the step function nodes to the iteration node
            iterNode.setPartialSolution(solutionSetNode, worksetNode);
            iterNode.setNextPartialSolution(
                    solutionSetDeltaNode, nextWorksetNode, defaultDataExchangeMode);

            // go over the contained data flow and mark the dynamic path nodes
            StaticDynamicPathIdentifier pathIdentifier =
                    new StaticDynamicPathIdentifier(iterNode.getCostWeight());
            iterNode.acceptForStepFunction(pathIdentifier);
        }
    }
}
