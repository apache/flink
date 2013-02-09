/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.compiler.postpass;

import java.util.Iterator;
import java.util.Map;

import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.contract.RecordContract;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.CompilerPostPassException;
import eu.stratosphere.pact.compiler.plan.SingleInputNode;
import eu.stratosphere.pact.compiler.plan.TwoInputNode;
import eu.stratosphere.pact.compiler.plan.candidate.BulkIterationPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.BulkPartialSolutionPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SourcePlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.UnionPlanNode;
import eu.stratosphere.pact.generic.contract.DualInputContract;
import eu.stratosphere.pact.generic.contract.SingleInputContract;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializerFactory;

/**
 * 
 */
public class PactRecordPostPass implements OptimizerPostPass
{
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.OptimizerPostPass#postPass(eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan)
	 */
	@Override
	public void postPass(OptimizedPlan plan) {
		for (SinkPlanNode sink : plan.getDataSinks()) {
			traverse(sink, null);
		}
	}
	
	protected void traverse(PlanNode node, KeySchema parentSchema) {
		// distinguish the node types
		if (node instanceof SinkPlanNode) {
			// sinks can derive type schema from partitioning and local ordering,
			// if that is set. otherwise they don't have and need schema information
			final SinkPlanNode sn = (SinkPlanNode) node;
			
			final Channel inchannel = sn.getInput();
			final KeySchema schema = new KeySchema();
			sn.postPassHelper = schema;
			
			final GenericDataSink pactSink = sn.getSinkNode().getPactContract();
			final Ordering partitioning = pactSink.getPartitionOrdering();
			final Ordering sorting = pactSink.getLocalOrder();
			
			try {
				if (partitioning != null) {
					addOrderingToSchema(partitioning, schema);
				}
				if (sorting != null) {
					addOrderingToSchema(sorting, schema);
				}
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("BUG: Conflicting information found when adding data sink types");
			}
			try {
				propagateToChannel(schema, inchannel);
			} catch (MissingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("BUG: Missing key type infomation for input to to data sink.");
			}
		}
		else if (node instanceof SourcePlanNode) {
			((SourcePlanNode) node).setSerializer(PactRecordSerializerFactory.get());
			// nothing else to be done here. the source has no input and no strategy itself
		}
		else if (node instanceof BulkIterationPlanNode) {
			BulkIterationPlanNode iterationNode = (BulkIterationPlanNode) node;
			
			// get the nodes current schema
			final KeySchema schema;
			if (iterationNode.postPassHelper == null) {
				schema = new KeySchema();
				iterationNode.postPassHelper = schema;
			} else {
				schema = (KeySchema) iterationNode.postPassHelper;
			}
			schema.increaseNumConnectionsThatContributed();
			
			// add the parent schema to the schema
			try {
				for (Map.Entry<Integer, Class<? extends Key>> entry : parentSchema) {
					final Integer pos = entry.getKey();
					schema.addType(pos, entry.getValue());
				}
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Conflicting key type information for field " + ex.getFieldNumber()
					+ " in node '" + iterationNode.getPactContract().getName() + "' propagated from successor node. " +
					"Conflicting types: " + ex.getPreviousType().getName() + " and " + ex.getNewType().getName() +
					". Most probable cause: Invalid constant field annotations.");
			}
			
			// check whether all outgoing channels have not yet contributed. come back later if not.
			if (schema.getNumConnectionsThatContributed() < iterationNode.getOutgoingChannels().size()) {
				return;
			}
			
			// set the serializer
			iterationNode.setSerializerForIterationChannel(PactRecordSerializerFactory.get());
			// traverse the step function
			traverse(iterationNode.getRootOfStepFunction(), schema);
			
			// take the schema from the partial solution node and add its fields to the iteration result schema.
			// input and output schema need to be identical, so this is essentially a sanity check
			KeySchema pss = (KeySchema) iterationNode.getPartialSolutionPlanNode().postPassHelper;
			try {
				for (Map.Entry<Integer, Class<? extends Key>> entry : pss) {
					final Integer pos = entry.getKey();
					schema.addType(pos, entry.getValue());
				}
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Conflicting key type information for field " + ex.getFieldNumber()
					+ " in node '" + iterationNode.getPactContract().getName() + "'. Contradicting types between the " +
					"result of the iteration and the partial solution schema: " + ex.getPreviousType().getName() + 
					" and " + ex.getNewType().getName() + 
					". Most probable cause: Invalid constant field annotations.");
			}
			
			// done, we can now propagate our info down
			try {
				propagateToChannel(schema, iterationNode.getInput());
			} catch (MissingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Could not set up runtime strategy for input channel to node '"
					+ iterationNode.getPactContract().getName() + "'. Missing type information for key field " + 
					ex.getFieldNumber());
			}
		}
		else if (node instanceof SingleInputPlanNode) {
			final SingleInputPlanNode sn = (SingleInputPlanNode) node;
			
			// get the nodes current schema
			final KeySchema schema;
			if (sn.postPassHelper == null) {
				schema = new KeySchema();
				sn.postPassHelper = schema;
			} else {
				schema = (KeySchema) sn.postPassHelper;
			}
			schema.increaseNumConnectionsThatContributed();
			
			// add the parent schema to the schema
			final SingleInputNode optNode = sn.getSingleInputNode();
			try {
				for (Map.Entry<Integer, Class<? extends Key>> entry : parentSchema) {
					final Integer pos = entry.getKey();
					if (optNode.isFieldConstant(0, pos)) {
						schema.addType(pos, entry.getValue());
					}
				}
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Conflicting key type information for field " + ex.getFieldNumber()
					+ " in node '" + optNode.getPactContract().getName() + "' propagated from successor node. " +
					"Conflicting types: " + ex.getPreviousType().getName() + " and " + ex.getNewType().getName() +
					". Most probable cause: Invalid constant field annotations.");
					
			}
			
			// check whether all outgoing channels have not yet contributed. come back later if not.
			if (schema.getNumConnectionsThatContributed() < sn.getOutgoingChannels().size()) {
				return;
			}
			
			// add the nodes local information. this automatically consistency checks
			final SingleInputContract<?> contract = (SingleInputContract<?>) optNode.getPactContract();
			if (! (contract instanceof RecordContract)) {
				throw new CompilerPostPassException("Error: Contract is not a Pact Record based contract. Wrong compiler invokation.");
			}
			final RecordContract recContract = (RecordContract) contract;
			final int[] localPositions = contract.getKeyColumns(0);
			final Class<? extends Key>[] types = recContract.getKeyClasses();
			try {
				for (int i = 0; i < localPositions.length; i++) {
					schema.addType(localPositions[i], types[i]);
				}
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Conflicting key type information for field " + ex.getFieldNumber()
					+ " in node '" + optNode.getPactContract().getName() + "' between types declared in the node's "
					+ "contract and types inferred from successor contracts. Conflicting types: "
					+ ex.getPreviousType().getName() + " and " + ex.getNewType().getName()
					+ ". Most probable cause: Invalid constant field annotations.");
					
			}
			
			// parameterize the node's driver strategy
			if (sn.getDriverStrategy().requiresComparator()) {
				try {
					sn.setComparator(createComparator(sn.getKeys(), sn.getSortOrders(), schema));
				} catch (MissingFieldTypeInfoException ex) {
					throw new CompilerPostPassException("Could not set up runtime strategy for node '" + 
						contract.getName() + "'. Missing type information for key field " + ex.getFieldNumber());
				}
			}
			
			// done, we can now propagate our info down
			try {
				propagateToChannel(schema, sn.getInput());
			} catch (MissingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Could not set up runtime strategy for input channel to node '"
					+ contract.getName() + "'. Missing type information for key field " + ex.getFieldNumber());
			}
		}
		else if (node instanceof DualInputPlanNode) {
			final DualInputPlanNode dn = (DualInputPlanNode) node;
			
			// get the nodes current schema
			final KeySchema schema1;
			final KeySchema schema2;
			if (dn.postPassHelper1 == null) {
				schema1 = new KeySchema();
				schema2 = new KeySchema();
				dn.postPassHelper1 = schema1;
				dn.postPassHelper2 = schema2;
			} else {
				schema1 = (KeySchema) dn.postPassHelper1;
				schema2 = (KeySchema) dn.postPassHelper2;
			}

			schema1.increaseNumConnectionsThatContributed();
			schema2.increaseNumConnectionsThatContributed();
			
			// add the parent schema to the schema
			final TwoInputNode optNode = dn.getTwoInputNode();
			try {
				for (Map.Entry<Integer, Class<? extends Key>> entry : parentSchema) {
					final Integer pos = entry.getKey();
					if (optNode.isFieldConstant(0, pos)) {
						schema1.addType(pos, entry.getValue());
					}
					if (optNode.isFieldConstant(1, pos)) {
						schema2.addType(pos, entry.getValue());
					}
				}
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Conflicting key type information for field " + ex.getFieldNumber()
					+ " in node '" + optNode.getPactContract().getName() + "' propagated from successor node. " +
					"Conflicting types: " + ex.getPreviousType().getName() + " and " + ex.getNewType().getName() +
					". Most probably cause: Invalid constant field annotations.");
					
			}
			
			// check whether all outgoing channels have not yet contributed. come back later if not.
			if (schema1.getNumConnectionsThatContributed() < dn.getOutgoingChannels().size()) {
				return;
			}
			
			// add the nodes local information. this automatically consistency checks
			final DualInputContract<?> contract = optNode.getPactContract();
			if (! (contract instanceof RecordContract)) {
				throw new CompilerPostPassException("Error: Contract is not a Pact Record based contract. Wrong compiler invokation.");
			}
			final RecordContract recContract = (RecordContract) contract;
			final int[] localPositions1 = contract.getKeyColumns(0);
			final int[] localPositions2 = contract.getKeyColumns(1);
			final Class<? extends Key>[] types = recContract.getKeyClasses();
			
			if (localPositions1.length != localPositions2.length) {
				throw new CompilerException("Error: The keys for the first and second input have a different number of fields.");
			}
			
			try {
				for (int i = 0; i < localPositions1.length; i++) {
					schema1.addType(localPositions1[i], types[i]);
				}
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Conflicting key type information for field " + ex.getFieldNumber()
					+ " in the first input of node '" + optNode.getPactContract().getName() + 
					"' between types declared in the node's contract and types inferred from successor contracts. " +
					"Conflicting types: " + ex.getPreviousType().getName() + " and " + ex.getNewType().getName()
					+ ". Most probable cause: Invalid constant field annotations.");
			}
			try {
				for (int i = 0; i < localPositions2.length; i++) {
					schema2.addType(localPositions2[i], types[i]);
				}
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Conflicting key type information for field " + ex.getFieldNumber()
					+ " in the second input of node '" + optNode.getPactContract().getName() + 
					"' between types declared in the node's contract and types inferred from successor contracts. " +
					"Conflicting types: " + ex.getPreviousType().getName() + " and " + ex.getNewType().getName()
					+ ". Most probable cause: Invalid constant field annotations.");
			}
			
			// parameterize the node's driver strategy
			if (dn.getDriverStrategy().requiresComparator()) {
				// set the individual comparators
				try {
					dn.setComparator1(createComparator(dn.getKeysForInput1(), dn.getSortOrders(), schema1));
					dn.setComparator2(createComparator(dn.getKeysForInput2(), dn.getSortOrders(), schema2));
				} catch (MissingFieldTypeInfoException ex) {
					throw new CompilerPostPassException("Could not set up runtime strategy for node '" + 
						contract.getName() + "'. Missing type information for key field " + ex.getFieldNumber());
				}
				
				// set the pair comparator
				dn.setPairComparator(PactRecordPairComparatorFactory.get());
			}
			
			// done, we can now propagate our info down
			try {
				propagateToChannel(schema1, dn.getInput1());
			} catch (MissingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Could not set up runtime strategy for the first input channel to node '"
					+ contract.getName() + "'. Missing type information for key field " + ex.getFieldNumber());
			}
			try {
				propagateToChannel(schema2, dn.getInput2());
			} catch (MissingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Could not set up runtime strategy for the second input channel to node '"
					+ contract.getName() + "'. Missing type information for key field " + ex.getFieldNumber());
			}
		}
		else if (node instanceof UnionPlanNode) {
			// only propagate the info down
			try {
				for (Iterator<Channel> channels = node.getInputs(); channels.hasNext(); ) {
					propagateToChannel(parentSchema, channels.next());
				}
			} catch (MissingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Could not set up runtime strategy for the input channel to " +
						" a union node. Missing type information for key field " + ex.getFieldNumber());
			}
		}
		else if (node instanceof BulkPartialSolutionPlanNode) {
			BulkPartialSolutionPlanNode psn = (BulkPartialSolutionPlanNode) node;
			
			// get the nodes current schema
			final KeySchema schema;
			if (psn.postPassHelper == null) {
				schema = new KeySchema();
				psn.postPassHelper = schema;
			} else {
				schema = (KeySchema) psn.postPassHelper;
			}
			schema.increaseNumConnectionsThatContributed();
			
			// add the parent schema to the schema
			try {
				for (Map.Entry<Integer, Class<? extends Key>> entry : parentSchema) {
					final Integer pos = entry.getKey();
					schema.addType(pos, entry.getValue());
				}
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Conflicting key type information for field " + ex.getFieldNumber()
					+ " in the partial solution of iteration '" + 
					psn.getPartialSolutionNode().getIterationNode().getPactContract().getName() +
					"', as propagated from the successor nodes. Conflicting types: " + ex.getPreviousType().getName() + 
					" and " + ex.getNewType().getName() + 
					". Most probable cause: Invalid constant field annotations.");
			}
		} else {
			throw new CompilerPostPassException("Unknown node type encountered: " + node.getClass().getName());
		}
	}
	
	private void propagateToChannel(KeySchema schema, Channel channel) throws MissingFieldTypeInfoException {
		// the serializer is always the same and always exists
		channel.setSerializer(PactRecordSerializerFactory.get());
		
		// parameterize the ship strategy
		if (channel.getShipStrategy().requiresComparator()) {
			channel.setShipStrategyComparator(
				createComparator(channel.getShipStrategyKeys(), channel.getShipStrategySortOrder(), schema));
		}
		
		// parameterize the local strategy
		if (channel.getLocalStrategy().requiresComparator()) {
			channel.setLocalStrategyComparator(
				createComparator(channel.getLocalStrategyKeys(), channel.getLocalStrategySortOrder(), schema));
		}
		
		// propagate the the channel's source model
		traverse(channel.getSource(), schema);
	}
	
	private void addOrderingToSchema(Ordering o, KeySchema schema) throws ConflictingFieldTypeInfoException {
		for (int i = 0; i < o.getNumberOfFields(); i++) {
			Integer pos = o.getFieldNumber(i);
			Class<? extends Key> type = o.getType(i);
			schema.addType(pos, type);
		}
	}
	
	private PactRecordComparatorFactory createComparator(FieldList fields, boolean[] directions, KeySchema schema)
	throws MissingFieldTypeInfoException
	{
		final int[] positions = fields.toArray();
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyTypes = new Class[fields.size()];
		
		for (int i = 0; i < fields.size(); i++) {
			Class<? extends Key> type = schema.getType(positions[i]);
			if (type == null) {
				throw new MissingFieldTypeInfoException(i);
			} else {
				keyTypes[i] = type;
			}
		}
		return new PactRecordComparatorFactory(positions, keyTypes, directions);
	}
}
