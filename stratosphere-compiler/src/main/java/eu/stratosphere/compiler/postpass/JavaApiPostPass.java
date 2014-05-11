/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.compiler.postpass;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.api.common.operators.BulkIteration;
import eu.stratosphere.api.common.operators.DeltaIteration;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.Union;
import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypePairComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.api.java.operators.translation.BinaryJavaPlanNode;
import eu.stratosphere.api.java.operators.translation.JavaPlanNode;
import eu.stratosphere.api.java.operators.translation.PlanBulkIterationOperator;
import eu.stratosphere.api.java.operators.translation.PlanDataSource;
import eu.stratosphere.api.java.operators.translation.PlanDeltaIterationOperator;
import eu.stratosphere.api.java.operators.translation.PlanGroupReduceOperator;
import eu.stratosphere.api.java.operators.translation.PlanUnwrappingReduceGroupOperator;
import eu.stratosphere.api.java.operators.translation.UnaryJavaPlanNode;
import eu.stratosphere.api.java.typeutils.AtomicType;
import eu.stratosphere.api.java.typeutils.CompositeType;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.api.java.typeutils.runtime.RuntimeComparatorFactory;
import eu.stratosphere.api.java.typeutils.runtime.RuntimePairComparatorFactory;
import eu.stratosphere.api.java.typeutils.runtime.RuntimeStatelessSerializerFactory;
import eu.stratosphere.api.java.typeutils.runtime.RuntimeStatefulSerializerFactory;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.CompilerPostPassException;
import eu.stratosphere.compiler.plan.BulkIterationPlanNode;
import eu.stratosphere.compiler.plan.BulkPartialSolutionPlanNode;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.NAryUnionPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plan.SinkPlanNode;
import eu.stratosphere.compiler.plan.SolutionSetPlanNode;
import eu.stratosphere.compiler.plan.SourcePlanNode;
import eu.stratosphere.compiler.plan.WorksetIterationPlanNode;
import eu.stratosphere.compiler.plan.WorksetPlanNode;
import eu.stratosphere.compiler.util.NoOpUnaryUdfOp;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

/**
 * The post-optimizer plan traversal. This traversal fills in the API specific utilities (serializers and
 * comparators).
 */
public class JavaApiPostPass implements OptimizerPostPass {
	
	private final Set<PlanNode> alreadyDone = new HashSet<PlanNode>();

	
	@Override
	public void postPass(OptimizedPlan plan) {
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
		}
		else if (node instanceof SourcePlanNode) {
			TypeInformation<?> typeInfo = getTypeInfoFromSource((SourcePlanNode) node);
			((SourcePlanNode) node).setSerializer(createSerializer(typeInfo));
		}
		else if (node instanceof BulkIterationPlanNode) {
			BulkIterationPlanNode iterationNode = (BulkIterationPlanNode) node;

			if (iterationNode.getRootOfStepFunction() instanceof NAryUnionPlanNode) {
				throw new CompilerException("Optimizer cannot compile an iteration step function where next partial solution is created by a Union node.");
			}
			
			// traverse the termination criterion for the first time. create schema only, no utilities. Needed in case of intermediate termination criterion
			if (iterationNode.getRootOfTerminationCriterion() != null) {
				SingleInputPlanNode addMapper = (SingleInputPlanNode) iterationNode.getRootOfTerminationCriterion();
				traverseChannel(addMapper.getInput());
			}

			PlanBulkIterationOperator<?> operator = (PlanBulkIterationOperator<?>) iterationNode.getPactContract();

			// set the serializer
			iterationNode.setSerializerForIterationChannel(createSerializer(operator.getReturnType()));

			// done, we can now propagate our info down
			traverseChannel(iterationNode.getInput());
			traverse(iterationNode.getRootOfStepFunction());
		}
		else if (node instanceof WorksetIterationPlanNode) {
			WorksetIterationPlanNode iterationNode = (WorksetIterationPlanNode) node;
			
			if (iterationNode.getNextWorkSetPlanNode() instanceof NAryUnionPlanNode) {
				throw new CompilerException("Optimizer cannot compile a workset iteration step function where the next workset is produced by a Union node.");
			}
			if (iterationNode.getSolutionSetDeltaPlanNode() instanceof NAryUnionPlanNode) {
				throw new CompilerException("Optimizer cannot compile a workset iteration step function where the solution set delta is produced by a Union node.");
			}
			
			PlanDeltaIterationOperator<?, ?> operator = (PlanDeltaIterationOperator<?, ?>) iterationNode.getPactContract();
			
			// set the serializers and comparators for the workset iteration
			iterationNode.setSolutionSetSerializer(createSerializer(operator.getSolutionsetType()));
			iterationNode.setWorksetSerializer(createSerializer(operator.getWorksetType()));
			iterationNode.setSolutionSetComparator(createComparator(operator.getSolutionsetType(),
					iterationNode.getSolutionSetKeyFields(), getSortOrders(iterationNode.getSolutionSetKeyFields(), null)));
			
			// traverse the inputs
			traverseChannel(iterationNode.getInput1());
			traverseChannel(iterationNode.getInput2());
			
			// traverse the step function
			traverse(iterationNode.getSolutionSetDeltaPlanNode());
			traverse(iterationNode.getNextWorkSetPlanNode());
		}
		else if (node instanceof SingleInputPlanNode) {
			SingleInputPlanNode sn = (SingleInputPlanNode) node;
			
			if (!(sn.getOptimizerNode().getPactContract() instanceof UnaryJavaPlanNode)) {
				
				// Special case for delta iterations
				if(sn.getOptimizerNode().getPactContract() instanceof NoOpUnaryUdfOp) {
					traverseChannel(sn.getInput());
					return;
				} else {
					throw new RuntimeException("Wrong operator type found in post pass.");
				}
			}
			
			UnaryJavaPlanNode<?, ?> javaNode = (UnaryJavaPlanNode<?, ?>) sn.getOptimizerNode().getPactContract();
			
			// parameterize the node's driver strategy
			if (sn.getDriverStrategy().requiresComparator()) {
				sn.setComparator(createComparator(javaNode.getInputType(), sn.getKeys(), 
					getSortOrders(sn.getKeys(), sn.getSortOrders())));
			}
			
			// done, we can now propagate our info down
			traverseChannel(sn.getInput());
			
			// don't forget the broadcast inputs
			for (Channel c: sn.getBroadcastInputs()) {
				traverseChannel(c);
			}
		}
		else if (node instanceof DualInputPlanNode) {
			DualInputPlanNode dn = (DualInputPlanNode) node;
			
			if (!(dn.getOptimizerNode().getPactContract() instanceof BinaryJavaPlanNode)) {
				throw new RuntimeException("Wrong operator type found in post pass.");
			}
			
			BinaryJavaPlanNode<?, ?, ?> javaNode = (BinaryJavaPlanNode<?, ?, ?>) dn.getOptimizerNode().getPactContract();
			
			// parameterize the node's driver strategy
			if (dn.getDriverStrategy().requiresComparator()) {
				dn.setComparator1(createComparator(javaNode.getInputType1(), dn.getKeysForInput1(), 
					getSortOrders(dn.getKeysForInput1(), dn.getSortOrders())));
				dn.setComparator2(createComparator(javaNode.getInputType2(), dn.getKeysForInput2(), 
						getSortOrders(dn.getKeysForInput2(), dn.getSortOrders())));

				dn.setPairComparator(createPairComparator(javaNode.getInputType1(), javaNode.getInputType2()));
				
			}
						
			traverseChannel(dn.getInput1());
			traverseChannel(dn.getInput2());
			
			// don't forget the broadcast inputs
			for (Channel c: dn.getBroadcastInputs()) {
				traverseChannel(c);
			}
			
		}
		// catch the sources of the iterative step functions
		else if (node instanceof BulkPartialSolutionPlanNode ||
				node instanceof SolutionSetPlanNode ||
				node instanceof WorksetPlanNode)
		{
			// Do nothing :D
		}
		else if (node instanceof NAryUnionPlanNode){
			// Traverse to all child channels
			for (Iterator<Channel> channels = node.getInputs(); channels.hasNext(); ) {
				traverseChannel(channels.next());
			}
		}
		else {
			throw new CompilerPostPassException("Unknown node type encountered: " + node.getClass().getName());
		}
	}
	
	private void traverseChannel(Channel channel) {
		
		PlanNode source = channel.getSource();
		Operator javaOp = source.getPactContract();
		
//		if (!(javaOp instanceof BulkIteration) && !(javaOp instanceof JavaPlanNode)) {
//			throw new RuntimeException("Wrong operator type found in post pass: " + javaOp);
//		}

		TypeInformation<?> type = null;

		if(javaOp instanceof PlanGroupReduceOperator && source.getDriverStrategy().equals(DriverStrategy.SORTED_GROUP_COMBINE)) {
			PlanGroupReduceOperator<?, ?> groupNode = (PlanGroupReduceOperator<?, ?>) javaOp;
			type = groupNode.getInputType();
		}
		else if(javaOp instanceof PlanUnwrappingReduceGroupOperator && source.getDriverStrategy().equals(DriverStrategy.SORTED_GROUP_COMBINE)) {
			PlanUnwrappingReduceGroupOperator<?, ?, ?> groupNode = (PlanUnwrappingReduceGroupOperator<?, ?, ?>) javaOp;
			type = groupNode.getInputType();
		}
		else if (javaOp instanceof JavaPlanNode<?>) {
			JavaPlanNode<?> javaNode = (JavaPlanNode<?>) javaOp;
			type = javaNode.getReturnType();
		} else if (javaOp instanceof BulkIteration.PartialSolutionPlaceHolder) {
			BulkIteration.PartialSolutionPlaceHolder partialSolutionPlaceHolder =
					(BulkIteration.PartialSolutionPlaceHolder) javaOp;
			type = ((PlanBulkIterationOperator<?>)partialSolutionPlaceHolder.getContainingBulkIteration()).getReturnType();
		} else if (javaOp instanceof DeltaIteration.SolutionSetPlaceHolder) {
			DeltaIteration.SolutionSetPlaceHolder solutionSetPlaceHolder =
					(DeltaIteration.SolutionSetPlaceHolder) javaOp;
			type = ((PlanDeltaIterationOperator<?, ?>) solutionSetPlaceHolder.getContainingWorksetIteration()).getReturnType();
		}  else if (javaOp instanceof DeltaIteration.WorksetPlaceHolder) {
			DeltaIteration.WorksetPlaceHolder worksetPlaceHolder =
					(DeltaIteration.WorksetPlaceHolder) javaOp;
			type = ((PlanDeltaIterationOperator<?, ?>) worksetPlaceHolder.getContainingWorksetIteration()).getReturnType();
		}  else if (javaOp instanceof NoOpUnaryUdfOp) {
			NoOpUnaryUdfOp op = (NoOpUnaryUdfOp) javaOp;
			if(op.getInput() instanceof JavaPlanNode<?>) { 
				JavaPlanNode<?> javaNode = (JavaPlanNode<?>) op.getInput();
				type = javaNode.getReturnType();
			}
		}else if(javaOp instanceof Union){
			// Union
			Operator op = channel.getSource().getInputs().next().getSource().getPactContract();
			if(op instanceof JavaPlanNode<?>){
				JavaPlanNode<?> javaNode = (JavaPlanNode<?>) op;
				type = javaNode.getReturnType();
			}
		}
		
		// the serializer always exists
		channel.setSerializer(createSerializer(type));
			
		// parameterize the ship strategy
		if (channel.getShipStrategy().requiresComparator()) {
			channel.setShipStrategyComparator(createComparator(type, channel.getShipStrategyKeys(), 
				getSortOrders(channel.getShipStrategyKeys(), channel.getShipStrategySortOrder())));
		}
			
		// parameterize the local strategy
		if (channel.getLocalStrategy().requiresComparator()) {
			channel.setLocalStrategyComparator(createComparator(type, channel.getLocalStrategyKeys(),
				getSortOrders(channel.getLocalStrategyKeys(), channel.getLocalStrategySortOrder())));
		}
		
		// descend to the channel's source
		traverse(channel.getSource());
	}
	
	
	@SuppressWarnings("unchecked")
	private static <T> TypeInformation<T> getTypeInfoFromSource(SourcePlanNode node) {
		Operator op = node.getOptimizerNode().getPactContract();
		
		if (op instanceof PlanDataSource) {
			return ((PlanDataSource<T>) op).getReturnType();
		} else {
			throw new RuntimeException("Wrong operator type found in post pass.");
		}
	}

	
	private static <T> TypeSerializerFactory<?> createSerializer(TypeInformation<T> typeInfo) {
		TypeSerializer<T> serializer = typeInfo.createSerializer();
		
		if (serializer.isStateful()) {
			return new RuntimeStatefulSerializerFactory<T>(serializer, typeInfo.getTypeClass());
		} else {
			return new RuntimeStatelessSerializerFactory<T>(serializer, typeInfo.getTypeClass());
		}
	}
	
	
	@SuppressWarnings("unchecked")
	private static <T> TypeComparatorFactory<?> createComparator(TypeInformation<T> typeInfo, FieldList keys, boolean[] sortOrder) {
		
		TypeComparator<T> comparator;
		if (typeInfo instanceof CompositeType) {
			comparator = ((CompositeType<T>) typeInfo).createComparator(keys.toArray(), sortOrder);
		}
		else if (typeInfo instanceof AtomicType) {
			// handle grouping of atomic types
			throw new UnsupportedOperationException("Grouping on atomic types is currently not implemented.");
		}
		else {
			throw new RuntimeException("Unrecognized type: " + typeInfo);
		}

		return new RuntimeComparatorFactory<T>(comparator);
	}
	
	private static <T1, T2> TypePairComparatorFactory<?,?> createPairComparator(TypeInformation<T1> typeInfo1, TypeInformation<T2> typeInfo2) {
		return new RuntimePairComparatorFactory<T1,T2>();
	}
	
	private static final boolean[] getSortOrders(FieldList keys, boolean[] orders) {
		if (orders == null) {
			orders = new boolean[keys.size()];
			Arrays.fill(orders, true);
		}
		return orders;
	}
}
