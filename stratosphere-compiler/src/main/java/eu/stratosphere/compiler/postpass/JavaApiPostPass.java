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

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.api.common.typeutils.Serializer;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypePairComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.api.java.operators.translation.BinaryJavaPlanNode;
import eu.stratosphere.api.java.operators.translation.JavaPlanNode;
import eu.stratosphere.api.java.operators.translation.PlanDataSource;
import eu.stratosphere.api.java.operators.translation.UnaryJavaPlanNode;
import eu.stratosphere.api.java.typeutils.AtomicType;
import eu.stratosphere.api.java.typeutils.CompositeType;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.api.java.typeutils.runtime.ReferenceWrappedComparator;
import eu.stratosphere.api.java.typeutils.runtime.ReferenceWrappedPairComparator.ReferenceWrappedPairComparatorFactory;
import eu.stratosphere.api.java.typeutils.runtime.ReferenceWrappedSerializer;
import eu.stratosphere.compiler.CompilerPostPassException;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plan.SinkPlanNode;
import eu.stratosphere.compiler.plan.SourcePlanNode;

public class JavaApiPostPass implements OptimizerPostPass {

	@Override
	public void postPass(OptimizedPlan plan) {
		for (SinkPlanNode sink : plan.getDataSinks()) {
			traverse(sink);
		}
	}
	

	protected void traverse(PlanNode node) {
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
//		else if (node instanceof BulkIterationPlanNode) {
//			BulkIterationPlanNode iterationNode = (BulkIterationPlanNode) node;
//			
//			// get the nodes current schema
//			T schema;
//			if (iterationNode.postPassHelper == null) {
//				schema = createEmptySchema();
//				iterationNode.postPassHelper = schema;
//			} else {
//				schema = (T) iterationNode.postPassHelper;
//			}
//			schema.increaseNumConnectionsThatContributed();
//			
//			// add the parent schema to the schema
//			if (propagateParentSchemaDown) {
//				addSchemaToSchema(parentSchema, schema, iterationNode.getPactContract().getName());
//			}
//			
//			// check whether all outgoing channels have not yet contributed. come back later if not.
//			if (schema.getNumConnectionsThatContributed() < iterationNode.getOutgoingChannels().size()) {
//				return;
//			}
//			
//			if (iterationNode.getRootOfStepFunction() instanceof NAryUnionPlanNode) {
//				throw new CompilerException("Optimizer cannot compile an iteration step function where next partial solution is created by a Union node.");
//			}
//			
//			// traverse the step function for the first time. create schema only, no utilities
//			traverse(iterationNode.getRootOfStepFunction(), schema, false);
//			
//			T pss = (T) iterationNode.getPartialSolutionPlanNode().postPassHelper;
//			if (pss == null) {
//				throw new CompilerException("Error in Optimizer Post Pass: Partial solution schema is null after first traversal of the step function.");
//			}
//			
//			// traverse the step function for the second time, taking the schema of the partial solution
//			traverse(iterationNode.getRootOfStepFunction(), pss, createUtilities);
//			
//			// take the schema from the partial solution node and add its fields to the iteration result schema.
//			// input and output schema need to be identical, so this is essentially a sanity check
//			addSchemaToSchema(pss, schema, iterationNode.getPactContract().getName());
//			
//			// set the serializer
//			if (createUtilities) {
//				iterationNode.setSerializerForIterationChannel(createSerializer(pss, iterationNode.getPartialSolutionPlanNode()));
//			}
//			
//			// done, we can now propagate our info down
//			try {
//				propagateToChannel(schema, iterationNode.getInput(), createUtilities);
//			} catch (MissingFieldTypeInfoException e) {
//				throw new CompilerPostPassException("Could not set up runtime strategy for input channel to node '"
//					+ iterationNode.getPactContract().getName() + "'. Missing type information for key field " + 
//					e.getFieldNumber());
//			}
//		}
//		else if (node instanceof WorksetIterationPlanNode) {
//			WorksetIterationPlanNode iterationNode = (WorksetIterationPlanNode) node;
//			
//			// get the nodes current schema
//			T schema;
//			if (iterationNode.postPassHelper == null) {
//				schema = createEmptySchema();
//				iterationNode.postPassHelper = schema;
//			} else {
//				schema = (T) iterationNode.postPassHelper;
//			}
//			schema.increaseNumConnectionsThatContributed();
//			
//			// add the parent schema to the schema (which refers to the solution set schema)
//			if (propagateParentSchemaDown) {
//				addSchemaToSchema(parentSchema, schema, iterationNode.getPactContract().getName());
//			}
//			
//			// check whether all outgoing channels have not yet contributed. come back later if not.
//			if (schema.getNumConnectionsThatContributed() < iterationNode.getOutgoingChannels().size()) {
//				return;
//			}
//			if (iterationNode.getNextWorkSetPlanNode() instanceof NAryUnionPlanNode) {
//				throw new CompilerException("Optimizer cannot compile a workset iteration step function where the next workset is produced by a Union node.");
//			}
//			if (iterationNode.getSolutionSetDeltaPlanNode() instanceof NAryUnionPlanNode) {
//				throw new CompilerException("Optimizer cannot compile a workset iteration step function where the solution set delta is produced by a Union node.");
//			}
//			
//			// traverse the step function
//			// pass an empty schema to the next workset and the parent schema to the solution set delta
//			// these first traversals are schema only
//			traverse(iterationNode.getNextWorkSetPlanNode(), createEmptySchema(), false);
//			traverse(iterationNode.getSolutionSetDeltaPlanNode(), schema, false);
//			
//			T wss = (T) iterationNode.getWorksetPlanNode().postPassHelper;
//			T sss = (T) iterationNode.getSolutionSetPlanNode().postPassHelper;
//			
//			if (wss == null) {
//				throw new CompilerException("Error in Optimizer Post Pass: Workset schema is null after first traversal of the step function.");
//			}
//			if (sss == null) {
//				throw new CompilerException("Error in Optimizer Post Pass: Solution set schema is null after first traversal of the step function.");
//			}
//			
//			// make the second pass and instantiate the utilities
//			traverse(iterationNode.getNextWorkSetPlanNode(), wss, createUtilities);
//			traverse(iterationNode.getSolutionSetDeltaPlanNode(), sss, createUtilities);
//			
//			// add the types from the solution set schema to the iteration's own schema. since
//			// the solution set input and the result must have the same schema, this acts as a sanity check.
//			try {
//				for (Map.Entry<Integer, X> entry : sss) {
//					Integer pos = entry.getKey();
//					schema.addType(pos, entry.getValue());
//				}
//			} catch (ConflictingFieldTypeInfoException e) {
//				throw new CompilerPostPassException("Conflicting type information for field " + e.getFieldNumber()
//					+ " in node '" + iterationNode.getPactContract().getName() + "'. Contradicting types between the " +
//					"result of the iteration and the solution set schema: " + e.getPreviousType() + 
//					" and " + e.getNewType() + ". Most probable cause: Invalid constant field annotations.");
//			}
//			
//			// set the serializers and comparators
//			if (createUtilities) {
//				WorksetIterationNode optNode = iterationNode.getIterationNode();
//				iterationNode.setWorksetSerializer(createSerializer(wss, iterationNode.getWorksetPlanNode()));
//				iterationNode.setSolutionSetSerializer(createSerializer(sss, iterationNode.getSolutionSetPlanNode()));
//				try {
//					iterationNode.setSolutionSetComparator(createComparator(optNode.getSolutionSetKeyFields(), null, sss));
//				} catch (MissingFieldTypeInfoException ex) {
//					throw new CompilerPostPassException("Could not set up the solution set for workset iteration '" + 
//							optNode.getPactContract().getName() + "'. Missing type information for key field " + ex.getFieldNumber() + '.');
//				}
//			}
//			
//			// done, we can now propagate our info down
//			try {
//				propagateToChannel(schema, iterationNode.getInitialSolutionSetInput(), createUtilities);
//				propagateToChannel(wss, iterationNode.getInitialWorksetInput(), createUtilities);
//			} catch (MissingFieldTypeInfoException ex) {
//				throw new CompilerPostPassException("Could not set up runtime strategy for input channel to node '"
//					+ iterationNode.getPactContract().getName() + "'. Missing type information for key field " + 
//					ex.getFieldNumber());
//			}
//		}
		else if (node instanceof SingleInputPlanNode) {
			SingleInputPlanNode sn = (SingleInputPlanNode) node;
			
			// get the nodes current schema
//			T schema;
//			if (sn.postPassHelper == null) {
//				schema = createEmptySchema();
//				sn.postPassHelper = schema;
//			} else {
//				schema = (T) sn.postPassHelper;
//			}
//			schema.increaseNumConnectionsThatContributed();
//			SingleInputNode optNode = sn.getSingleInputNode();
//			
//			// add the parent schema to the schema
//			if (propagateParentSchemaDown) {
//				addSchemaToSchema(parentSchema, schema, optNode, 0);
//			}
//			
//			// check whether all outgoing channels have not yet contributed. come back later if not.
//			if (schema.getNumConnectionsThatContributed() < sn.getOutgoingChannels().size()) {
//				return;
//			}
			
			if (!(sn.getOptimizerNode().getPactContract() instanceof UnaryJavaPlanNode)) {
				throw new RuntimeException("Wrong operator type found in post pass.");
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
//			DualInputPlanNode dn = (DualInputPlanNode) node;
//			
//			// get the nodes current schema
//			T schema1;
//			T schema2;
//			if (dn.postPassHelper1 == null) {
//				schema1 = createEmptySchema();
//				schema2 = createEmptySchema();
//				dn.postPassHelper1 = schema1;
//				dn.postPassHelper2 = schema2;
//			} else {
//				schema1 = (T) dn.postPassHelper1;
//				schema2 = (T) dn.postPassHelper2;
//			}
//
//			schema1.increaseNumConnectionsThatContributed();
//			schema2.increaseNumConnectionsThatContributed();
//			TwoInputNode optNode = dn.getTwoInputNode();
//			
//			// add the parent schema to the schema
//			if (propagateParentSchemaDown) {
//				addSchemaToSchema(parentSchema, schema1, optNode, 0);
//				addSchemaToSchema(parentSchema, schema2, optNode, 1);
//			}
//			
//			// check whether all outgoing channels have not yet contributed. come back later if not.
//			if (schema1.getNumConnectionsThatContributed() < dn.getOutgoingChannels().size()) {
//				return;
//			}
//			
//			// add the nodes local information
//			try {
//				getDualInputNodeSchema(dn, schema1, schema2);
//			} catch (ConflictingFieldTypeInfoException e) {
//				throw new CompilerPostPassException(getConflictingTypeErrorMessage(e, optNode.getPactContract().getName()));
//			}
//			
//			// parameterize the node's driver strategy
//			if (createUtilities) {
//				if (dn.getDriverStrategy().requiresComparator()) {
//					// set the individual comparators
//					try {
//						dn.setComparator1(createComparator(dn.getKeysForInput1(), dn.getSortOrders(), schema1));
//						dn.setComparator2(createComparator(dn.getKeysForInput2(), dn.getSortOrders(), schema2));
//					} catch (MissingFieldTypeInfoException e) {
//						throw new CompilerPostPassException("Could not set up runtime strategy for node '" + 
//								optNode.getPactContract().getName() + "'. Missing type information for field " + e.getFieldNumber());
//					}
//					
//					// set the pair comparator
//					try {
//						dn.setPairComparator(createPairComparator(dn.getKeysForInput1(), dn.getKeysForInput2(), 
//							dn.getSortOrders(), schema1, schema2));
//					} catch (MissingFieldTypeInfoException e) {
//						throw new CompilerPostPassException("Could not set up runtime strategy for node '" + 
//								optNode.getPactContract().getName() + "'. Missing type information for field " + e.getFieldNumber());
//					}
//					
//				}
//			}
//			
//			// done, we can now propagate our info down
//			try {
//				propagateToChannel(schema1, dn.getInput1(), createUtilities);
//			} catch (MissingFieldTypeInfoException e) {
//				throw new CompilerPostPassException("Could not set up runtime strategy for the first input channel to node '"
//					+ optNode.getPactContract().getName() + "'. Missing type information for field " + e.getFieldNumber());
//			}
//			try {
//				propagateToChannel(schema2, dn.getInput2(), createUtilities);
//			} catch (MissingFieldTypeInfoException e) {
//				throw new CompilerPostPassException("Could not set up runtime strategy for the second input channel to node '"
//					+ optNode.getPactContract().getName() + "'. Missing type information for field " + e.getFieldNumber());
//			}
//			
//			// don't forget the broadcast inputs
//			for (Channel c: dn.getBroadcastInputs()) {
//				try {
//					propagateToChannel(createEmptySchema(), c, createUtilities);
//				} catch (MissingFieldTypeInfoException e) {
//					throw new CompilerPostPassException("Could not set up runtime strategy for broadcast channel in node '" +
//						optNode.getPactContract().getName() + "'. Missing type information for field " + e.getFieldNumber());
//				}
//			}
//		}
//		else if (node instanceof NAryUnionPlanNode) {
//			// only propagate the info down
//			try {
//				for (Iterator<Channel> channels = node.getInputs(); channels.hasNext(); ) {
//					propagateToChannel(parentSchema, channels.next(), createUtilities);
//				}
//			} catch (MissingFieldTypeInfoException ex) {
//				throw new CompilerPostPassException("Could not set up runtime strategy for the input channel to " +
//						" a union node. Missing type information for field " + ex.getFieldNumber());
//			}
//		}
//		// catch the sources of the iterative step functions
//		else if (node instanceof BulkPartialSolutionPlanNode || 
//				 node instanceof SolutionSetPlanNode ||
//				 node instanceof WorksetPlanNode)
//		{
//			// get the nodes current schema
//			T schema;
//			String name;
//			if (node instanceof BulkPartialSolutionPlanNode) {
//				BulkPartialSolutionPlanNode psn = (BulkPartialSolutionPlanNode) node;
//				if (psn.postPassHelper == null) {
//					schema = createEmptySchema();
//					psn.postPassHelper = schema;
//				} else {
//					schema = (T) psn.postPassHelper;
//				}
//				name = "partial solution of bulk iteration '" +
//					psn.getPartialSolutionNode().getIterationNode().getPactContract().getName() + "'";
//			}
//			else if (node instanceof SolutionSetPlanNode) {
//				SolutionSetPlanNode ssn = (SolutionSetPlanNode) node;
//				if (ssn.postPassHelper == null) {
//					schema = createEmptySchema();
//					ssn.postPassHelper = schema;
//				} else {
//					schema = (T) ssn.postPassHelper;
//				}
//				name = "solution set of workset iteration '" +
//						ssn.getSolutionSetNode().getIterationNode().getPactContract().getName() + "'";
//			}
//			else if (node instanceof WorksetPlanNode) {
//				WorksetPlanNode wsn = (WorksetPlanNode) node;
//				if (wsn.postPassHelper == null) {
//					schema = createEmptySchema();
//					wsn.postPassHelper = schema;
//				} else {
//					schema = (T) wsn.postPassHelper;
//				}
//				name = "workset of workset iteration '" +
//						wsn.getWorksetNode().getIterationNode().getPactContract().getName() + "'";
//			} else {
//				throw new CompilerException();
//			}
//			
//			schema.increaseNumConnectionsThatContributed();
//			
//			// add the parent schema to the schema
//			addSchemaToSchema(parentSchema, schema, name);
//		}
		else {
			throw new CompilerPostPassException("Unknown node type encountered: " + node.getClass().getName());
		}
	}
	
	private void traverseChannel(Channel channel) {
		
		PlanNode source = channel.getSource();
		Operator javaOp = source.getPactContract();
		
		if (!(javaOp instanceof JavaPlanNode)) {
			throw new RuntimeException("Wrong operator type found in post pass.");
		}
		
		JavaPlanNode<?> javaNode = (JavaPlanNode<?>) javaOp;
		TypeInformation<?> type = javaNode.getReturnType();
		
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
		Serializer<T> serializer = typeInfo.createSerializer();
		
		ReferenceWrappedSerializer<T> wrapper = new ReferenceWrappedSerializer<T>(serializer);
		
		return new ReferenceWrappedSerializer.ReferenceWrappedSerializerFactory<T>(wrapper);
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
		
		ReferenceWrappedComparator<T> wrappingComparator = new ReferenceWrappedComparator<T>(comparator);
		
		return new ReferenceWrappedComparator.ReferenceWrappedComparatorFactory<T>(wrappingComparator);
	}
	
	private static <T1, T2> TypePairComparatorFactory<?,?> createPairComparator(TypeInformation<T1> typeInfo1, TypeInformation<T2> typeInfo2) {
		return new ReferenceWrappedPairComparatorFactory<T1,T2>();
	}
	
	private static final boolean[] getSortOrders(FieldList keys, boolean[] orders) {
		if (orders == null) {
			orders = new boolean[keys.size()];
			Arrays.fill(orders, true);
		}
		return orders;
	}
}
