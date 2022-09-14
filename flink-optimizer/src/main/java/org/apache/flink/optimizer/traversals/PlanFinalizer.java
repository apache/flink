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

import org.apache.flink.api.common.Plan;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.plan.BinaryUnionPlanNode;
import org.apache.flink.optimizer.plan.BulkIterationPlanNode;
import org.apache.flink.optimizer.plan.BulkPartialSolutionPlanNode;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.IterationPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.SolutionSetPlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.optimizer.plan.WorksetPlanNode;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.util.Visitor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This visitor traverses the selected execution plan and finalizes it:
 *
 * <ul>
 *   <li>The graph of nodes is double-linked (links from child to parent are inserted).
 *   <li>If unions join static and dynamic paths, the cache is marked as a memory consumer.
 *   <li>Relative memory fractions are assigned to all nodes.
 *   <li>All nodes are collected into a set.
 * </ul>
 */
public class PlanFinalizer implements Visitor<PlanNode> {

    private final Set<PlanNode> allNodes; // a set of all nodes in the optimizer plan

    private final List<SourcePlanNode> sources; // all data source nodes in the optimizer plan

    private final List<SinkPlanNode> sinks; // all data sink nodes in the optimizer plan

    private final Deque<IterationPlanNode> stackOfIterationNodes;

    private int memoryConsumerWeights; // a counter of all memory consumers

    /** Creates a new plan finalizer. */
    public PlanFinalizer() {
        this.allNodes = new HashSet<PlanNode>();
        this.sources = new ArrayList<SourcePlanNode>();
        this.sinks = new ArrayList<SinkPlanNode>();
        this.stackOfIterationNodes = new ArrayDeque<IterationPlanNode>();
    }

    public OptimizedPlan createFinalPlan(
            List<SinkPlanNode> sinks, String jobName, Plan originalPlan) {
        this.memoryConsumerWeights = 0;

        // traverse the graph
        for (SinkPlanNode node : sinks) {
            node.accept(this);
        }

        // assign the memory to each node
        if (this.memoryConsumerWeights > 0) {
            for (PlanNode node : this.allNodes) {
                // assign memory to the driver strategy of the node
                final int consumerWeight = node.getMemoryConsumerWeight();
                if (consumerWeight > 0) {
                    final double relativeMem = (double) consumerWeight / this.memoryConsumerWeights;
                    node.setRelativeMemoryPerSubtask(relativeMem);
                    if (Optimizer.LOG.isDebugEnabled()) {
                        Optimizer.LOG.debug(
                                "Assigned "
                                        + relativeMem
                                        + " of total memory to each subtask of "
                                        + node.getProgramOperator().getName()
                                        + ".");
                    }
                }

                // assign memory to the local and global strategies of the channels
                for (Channel c : node.getInputs()) {
                    if (c.getLocalStrategy().dams()) {
                        final double relativeMem = 1.0 / this.memoryConsumerWeights;
                        c.setRelativeMemoryLocalStrategy(relativeMem);
                        if (Optimizer.LOG.isDebugEnabled()) {
                            Optimizer.LOG.debug(
                                    "Assigned "
                                            + relativeMem
                                            + " of total memory to each local strategy "
                                            + "instance of "
                                            + c
                                            + ".");
                        }
                    }
                    if (c.getTempMode() != TempMode.NONE) {
                        final double relativeMem = 1.0 / this.memoryConsumerWeights;
                        c.setRelativeTempMemory(relativeMem);
                        if (Optimizer.LOG.isDebugEnabled()) {
                            Optimizer.LOG.debug(
                                    "Assigned "
                                            + relativeMem
                                            + " of total memory to each instance of the temp "
                                            + "table for "
                                            + c
                                            + ".");
                        }
                    }
                }
            }
        }
        return new OptimizedPlan(this.sources, this.sinks, this.allNodes, jobName, originalPlan);
    }

    @Override
    public boolean preVisit(PlanNode visitable) {
        // if we come here again, prevent a further descend
        if (!this.allNodes.add(visitable)) {
            return false;
        }

        if (visitable instanceof SinkPlanNode) {
            this.sinks.add((SinkPlanNode) visitable);
        } else if (visitable instanceof SourcePlanNode) {
            this.sources.add((SourcePlanNode) visitable);
        } else if (visitable instanceof BinaryUnionPlanNode) {
            BinaryUnionPlanNode unionNode = (BinaryUnionPlanNode) visitable;
            if (unionNode.unionsStaticAndDynamicPath()) {
                unionNode.setDriverStrategy(DriverStrategy.UNION_WITH_CACHED);
            }
        } else if (visitable instanceof BulkPartialSolutionPlanNode) {
            // tell the partial solution about the iteration node that contains it
            final BulkPartialSolutionPlanNode pspn = (BulkPartialSolutionPlanNode) visitable;
            final IterationPlanNode iteration = this.stackOfIterationNodes.peekLast();

            // sanity check!
            if (!(iteration instanceof BulkIterationPlanNode)) {
                throw new CompilerException(
                        "Bug: Error finalizing the plan. "
                                + "Cannot associate the node for a partial solutions with its containing iteration.");
            }
            pspn.setContainingIterationNode((BulkIterationPlanNode) iteration);
        } else if (visitable instanceof WorksetPlanNode) {
            // tell the partial solution about the iteration node that contains it
            final WorksetPlanNode wspn = (WorksetPlanNode) visitable;
            final IterationPlanNode iteration = this.stackOfIterationNodes.peekLast();

            // sanity check!
            if (!(iteration instanceof WorksetIterationPlanNode)) {
                throw new CompilerException(
                        "Bug: Error finalizing the plan. "
                                + "Cannot associate the node for a partial solutions with its containing iteration.");
            }
            wspn.setContainingIterationNode((WorksetIterationPlanNode) iteration);
        } else if (visitable instanceof SolutionSetPlanNode) {
            // tell the partial solution about the iteration node that contains it
            final SolutionSetPlanNode sspn = (SolutionSetPlanNode) visitable;
            final IterationPlanNode iteration = this.stackOfIterationNodes.peekLast();

            // sanity check!
            if (!(iteration instanceof WorksetIterationPlanNode)) {
                throw new CompilerException(
                        "Bug: Error finalizing the plan. "
                                + "Cannot associate the node for a partial solutions with its containing iteration.");
            }
            sspn.setContainingIterationNode((WorksetIterationPlanNode) iteration);
        }

        // double-connect the connections. previously, only parents knew their children, because
        // one child candidate could have been referenced by multiple parents.
        for (Channel conn : visitable.getInputs()) {
            conn.setTarget(visitable);
            conn.getSource().addOutgoingChannel(conn);
        }

        for (Channel c : visitable.getBroadcastInputs()) {
            c.setTarget(visitable);
            c.getSource().addOutgoingChannel(c);
        }

        // count the memory consumption
        this.memoryConsumerWeights += visitable.getMemoryConsumerWeight();
        for (Channel c : visitable.getInputs()) {
            if (c.getLocalStrategy().dams()) {
                this.memoryConsumerWeights++;
            }
            if (c.getTempMode() != TempMode.NONE) {
                this.memoryConsumerWeights++;
            }
        }
        for (Channel c : visitable.getBroadcastInputs()) {
            if (c.getLocalStrategy().dams()) {
                this.memoryConsumerWeights++;
            }
            if (c.getTempMode() != TempMode.NONE) {
                this.memoryConsumerWeights++;
            }
        }

        // pass the visitor to the iteration's step function
        if (visitable instanceof IterationPlanNode) {
            // push the iteration node onto the stack
            final IterationPlanNode iterNode = (IterationPlanNode) visitable;
            this.stackOfIterationNodes.addLast(iterNode);

            // recurse
            ((IterationPlanNode) visitable).acceptForStepFunction(this);

            // pop the iteration node from the stack
            this.stackOfIterationNodes.removeLast();
        }
        return true;
    }

    @Override
    public void postVisit(PlanNode visitable) {}
}
