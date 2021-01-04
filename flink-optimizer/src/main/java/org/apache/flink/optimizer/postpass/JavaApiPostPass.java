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

package org.apache.flink.optimizer.postpass;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.base.BulkIterationBase;
import org.apache.flink.api.common.operators.base.DeltaIterationBase;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.java.operators.translation.PlanUnwrappingReduceGroupOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.runtime.RuntimeComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.RuntimePairComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.RuntimeSerializerFactory;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.CompilerPostPassException;
import org.apache.flink.optimizer.plan.BulkIterationPlanNode;
import org.apache.flink.optimizer.plan.BulkPartialSolutionPlanNode;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.NAryUnionPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.SolutionSetPlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.optimizer.plan.WorksetPlanNode;
import org.apache.flink.optimizer.util.NoOpUnaryUdfOp;
import org.apache.flink.runtime.operators.DriverStrategy;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * The post-optimizer plan traversal. This traversal fills in the API specific utilities
 * (serializers and comparators).
 */
public class JavaApiPostPass implements OptimizerPostPass {

    private final Set<PlanNode> alreadyDone = new HashSet<PlanNode>();

    private ExecutionConfig executionConfig = null;

    @Override
    public void postPass(OptimizedPlan plan) {

        executionConfig = plan.getOriginalPlan().getExecutionConfig();

        for (SinkPlanNode sink : plan.getDataSinks()) {
            traverse(sink);
        }
    }

    protected void traverse(PlanNode node) {
        if (!alreadyDone.add(node)) {
            // already worked on that one
            return;
        }

        // distinguish the node types
        if (node instanceof SinkPlanNode) {
            // descend to the input channel
            SinkPlanNode sn = (SinkPlanNode) node;
            Channel inchannel = sn.getInput();
            traverseChannel(inchannel);
        } else if (node instanceof SourcePlanNode) {
            TypeInformation<?> typeInfo = getTypeInfoFromSource((SourcePlanNode) node);
            ((SourcePlanNode) node).setSerializer(createSerializer(typeInfo));
        } else if (node instanceof BulkIterationPlanNode) {
            BulkIterationPlanNode iterationNode = (BulkIterationPlanNode) node;

            if (iterationNode.getRootOfStepFunction() instanceof NAryUnionPlanNode) {
                throw new CompilerException(
                        "Optimizer cannot compile an iteration step function where next partial solution is created by a Union node.");
            }

            // traverse the termination criterion for the first time. create schema only, no
            // utilities. Needed in case of intermediate termination criterion
            if (iterationNode.getRootOfTerminationCriterion() != null) {
                SingleInputPlanNode addMapper =
                        (SingleInputPlanNode) iterationNode.getRootOfTerminationCriterion();
                traverseChannel(addMapper.getInput());
            }

            BulkIterationBase<?> operator =
                    (BulkIterationBase<?>) iterationNode.getProgramOperator();

            // set the serializer
            iterationNode.setSerializerForIterationChannel(
                    createSerializer(operator.getOperatorInfo().getOutputType()));

            // done, we can now propagate our info down
            traverseChannel(iterationNode.getInput());
            traverse(iterationNode.getRootOfStepFunction());
        } else if (node instanceof WorksetIterationPlanNode) {
            WorksetIterationPlanNode iterationNode = (WorksetIterationPlanNode) node;

            if (iterationNode.getNextWorkSetPlanNode() instanceof NAryUnionPlanNode) {
                throw new CompilerException(
                        "Optimizer cannot compile a workset iteration step function where the next workset is produced by a Union node.");
            }
            if (iterationNode.getSolutionSetDeltaPlanNode() instanceof NAryUnionPlanNode) {
                throw new CompilerException(
                        "Optimizer cannot compile a workset iteration step function where the solution set delta is produced by a Union node.");
            }

            DeltaIterationBase<?, ?> operator =
                    (DeltaIterationBase<?, ?>) iterationNode.getProgramOperator();

            // set the serializers and comparators for the workset iteration
            iterationNode.setSolutionSetSerializer(
                    createSerializer(operator.getOperatorInfo().getFirstInputType()));
            iterationNode.setWorksetSerializer(
                    createSerializer(operator.getOperatorInfo().getSecondInputType()));
            iterationNode.setSolutionSetComparator(
                    createComparator(
                            operator.getOperatorInfo().getFirstInputType(),
                            iterationNode.getSolutionSetKeyFields(),
                            getSortOrders(iterationNode.getSolutionSetKeyFields(), null)));

            // traverse the inputs
            traverseChannel(iterationNode.getInput1());
            traverseChannel(iterationNode.getInput2());

            // traverse the step function
            traverse(iterationNode.getSolutionSetDeltaPlanNode());
            traverse(iterationNode.getNextWorkSetPlanNode());
        } else if (node instanceof SingleInputPlanNode) {
            SingleInputPlanNode sn = (SingleInputPlanNode) node;

            if (!(sn.getOptimizerNode().getOperator() instanceof SingleInputOperator)) {

                // Special case for delta iterations
                if (sn.getOptimizerNode().getOperator() instanceof NoOpUnaryUdfOp) {
                    traverseChannel(sn.getInput());
                    return;
                } else {
                    throw new RuntimeException("Wrong operator type found in post pass.");
                }
            }

            SingleInputOperator<?, ?, ?> singleInputOperator =
                    (SingleInputOperator<?, ?, ?>) sn.getOptimizerNode().getOperator();

            // parameterize the node's driver strategy
            for (int i = 0; i < sn.getDriverStrategy().getNumRequiredComparators(); i++) {
                sn.setComparator(
                        createComparator(
                                singleInputOperator.getOperatorInfo().getInputType(),
                                sn.getKeys(i),
                                getSortOrders(sn.getKeys(i), sn.getSortOrders(i))),
                        i);
            }
            // done, we can now propagate our info down
            traverseChannel(sn.getInput());

            // don't forget the broadcast inputs
            for (Channel c : sn.getBroadcastInputs()) {
                traverseChannel(c);
            }
        } else if (node instanceof DualInputPlanNode) {
            DualInputPlanNode dn = (DualInputPlanNode) node;

            if (!(dn.getOptimizerNode().getOperator() instanceof DualInputOperator)) {
                throw new RuntimeException("Wrong operator type found in post pass.");
            }

            DualInputOperator<?, ?, ?, ?> dualInputOperator =
                    (DualInputOperator<?, ?, ?, ?>) dn.getOptimizerNode().getOperator();

            // parameterize the node's driver strategy
            if (dn.getDriverStrategy().getNumRequiredComparators() > 0) {
                dn.setComparator1(
                        createComparator(
                                dualInputOperator.getOperatorInfo().getFirstInputType(),
                                dn.getKeysForInput1(),
                                getSortOrders(dn.getKeysForInput1(), dn.getSortOrders())));
                dn.setComparator2(
                        createComparator(
                                dualInputOperator.getOperatorInfo().getSecondInputType(),
                                dn.getKeysForInput2(),
                                getSortOrders(dn.getKeysForInput2(), dn.getSortOrders())));

                dn.setPairComparator(
                        createPairComparator(
                                dualInputOperator.getOperatorInfo().getFirstInputType(),
                                dualInputOperator.getOperatorInfo().getSecondInputType()));
            }

            traverseChannel(dn.getInput1());
            traverseChannel(dn.getInput2());

            // don't forget the broadcast inputs
            for (Channel c : dn.getBroadcastInputs()) {
                traverseChannel(c);
            }

        }
        // catch the sources of the iterative step functions
        else if (node instanceof BulkPartialSolutionPlanNode
                || node instanceof SolutionSetPlanNode
                || node instanceof WorksetPlanNode) {
            // Do nothing :D
        } else if (node instanceof NAryUnionPlanNode) {
            // Traverse to all child channels
            for (Channel channel : node.getInputs()) {
                traverseChannel(channel);
            }
        } else {
            throw new CompilerPostPassException(
                    "Unknown node type encountered: " + node.getClass().getName());
        }
    }

    private void traverseChannel(Channel channel) {

        PlanNode source = channel.getSource();
        Operator<?> javaOp = source.getProgramOperator();

        //		if (!(javaOp instanceof BulkIteration) && !(javaOp instanceof JavaPlanNode)) {
        //			throw new RuntimeException("Wrong operator type found in post pass: " + javaOp);
        //		}

        TypeInformation<?> type = javaOp.getOperatorInfo().getOutputType();

        if (javaOp instanceof GroupReduceOperatorBase
                && (source.getDriverStrategy() == DriverStrategy.SORTED_GROUP_COMBINE
                        || source.getDriverStrategy() == DriverStrategy.ALL_GROUP_REDUCE_COMBINE)) {
            GroupReduceOperatorBase<?, ?, ?> groupNode = (GroupReduceOperatorBase<?, ?, ?>) javaOp;
            type = groupNode.getInput().getOperatorInfo().getOutputType();
        } else if (javaOp instanceof PlanUnwrappingReduceGroupOperator
                && source.getDriverStrategy().equals(DriverStrategy.SORTED_GROUP_COMBINE)) {
            PlanUnwrappingReduceGroupOperator<?, ?, ?> groupNode =
                    (PlanUnwrappingReduceGroupOperator<?, ?, ?>) javaOp;
            type = groupNode.getInput().getOperatorInfo().getOutputType();
        }

        // the serializer always exists
        channel.setSerializer(createSerializer(type));

        // parameterize the ship strategy
        if (channel.getShipStrategy().requiresComparator()) {
            channel.setShipStrategyComparator(
                    createComparator(
                            type,
                            channel.getShipStrategyKeys(),
                            getSortOrders(
                                    channel.getShipStrategyKeys(),
                                    channel.getShipStrategySortOrder())));
        }

        // parameterize the local strategy
        if (channel.getLocalStrategy().requiresComparator()) {
            channel.setLocalStrategyComparator(
                    createComparator(
                            type,
                            channel.getLocalStrategyKeys(),
                            getSortOrders(
                                    channel.getLocalStrategyKeys(),
                                    channel.getLocalStrategySortOrder())));
        }

        // descend to the channel's source
        traverse(channel.getSource());
    }

    @SuppressWarnings("unchecked")
    private static <T> TypeInformation<T> getTypeInfoFromSource(SourcePlanNode node) {
        Operator<?> op = node.getOptimizerNode().getOperator();

        if (op instanceof GenericDataSourceBase) {
            return ((GenericDataSourceBase<T, ?>) op).getOperatorInfo().getOutputType();
        } else {
            throw new RuntimeException("Wrong operator type found in post pass.");
        }
    }

    private <T> TypeSerializerFactory<?> createSerializer(TypeInformation<T> typeInfo) {
        TypeSerializer<T> serializer = typeInfo.createSerializer(executionConfig);

        return new RuntimeSerializerFactory<T>(serializer, typeInfo.getTypeClass());
    }

    @SuppressWarnings("unchecked")
    private <T> TypeComparatorFactory<?> createComparator(
            TypeInformation<T> typeInfo, FieldList keys, boolean[] sortOrder) {

        TypeComparator<T> comparator;
        if (typeInfo instanceof CompositeType) {
            comparator =
                    ((CompositeType<T>) typeInfo)
                            .createComparator(keys.toArray(), sortOrder, 0, executionConfig);
        } else if (typeInfo instanceof AtomicType) {
            // handle grouping of atomic types
            comparator = ((AtomicType<T>) typeInfo).createComparator(sortOrder[0], executionConfig);
        } else {
            throw new RuntimeException("Unrecognized type: " + typeInfo);
        }

        return new RuntimeComparatorFactory<T>(comparator);
    }

    private static <T1 extends Tuple, T2 extends Tuple>
            TypePairComparatorFactory<T1, T2> createPairComparator(
                    TypeInformation<?> typeInfo1, TypeInformation<?> typeInfo2) {
        //		@SuppressWarnings("unchecked")
        //		TupleTypeInfo<T1> info1 = (TupleTypeInfo<T1>) typeInfo1;
        //		@SuppressWarnings("unchecked")
        //		TupleTypeInfo<T2> info2 = (TupleTypeInfo<T2>) typeInfo2;

        return new RuntimePairComparatorFactory<T1, T2>();
    }

    private static final boolean[] getSortOrders(FieldList keys, boolean[] orders) {
        if (orders == null) {
            orders = new boolean[keys.size()];
            Arrays.fill(orders, true);
        }
        return orders;
    }
}
