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
import eu.stratosphere.pact.compiler.plan.WorksetIterationNode;
import eu.stratosphere.pact.compiler.plan.candidate.BulkIterationPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.BulkPartialSolutionPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SolutionSetPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SourcePlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.UnionPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.WorksetIterationPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.WorksetPlanNode;
import eu.stratosphere.pact.generic.contract.DualInputContract;
import eu.stratosphere.pact.generic.contract.SingleInputContract;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializerFactory;

/**
 * 
 */
public class PactRecordPostPass implements OptimizerPostPass {
	
	@Override
	public void postPass(OptimizedPlan plan) {
		for (SinkPlanNode sink : plan.getDataSinks()) {
			traverse(sink, null, true);
		}
	}
	
	protected void traverse(PlanNode node, KeySchema parentSchema, boolean createUtilities) {
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
				propagateToChannel(schema, inchannel, createUtilities);
			} catch (MissingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("BUG: Missing key type infomation for input to to data sink.");
			}
		}
		else if (node instanceof SourcePlanNode) {
			if (createUtilities) {
				((SourcePlanNode) node).setSerializer(PactRecordSerializerFactory.get());
				// nothing else to be done here. the source has no input and no strategy itself
			}
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
			
			// traverse the step function for the first time. create schema only, no utilities
			traverse(iterationNode.getRootOfStepFunction(), schema, false);
			
			KeySchema pss = (KeySchema) iterationNode.getPartialSolutionPlanNode().postPassHelper;
			if (pss == null) {
				throw new CompilerException("Error in Optimizer Post Pass: Partial solution schema is null after first traversal of the step function.");
			}
			
			// traverse the step function for the second time, taking the schema of the partial solution
			traverse(iterationNode.getRootOfStepFunction(), pss, createUtilities);
			
			// take the schema from the partial solution node and add its fields to the iteration result schema.
			// input and output schema need to be identical, so this is essentially a sanity check
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
			
			// set the serializer
			if (createUtilities) {
				iterationNode.setSerializerForIterationChannel(PactRecordSerializerFactory.get());
			}
			
			// done, we can now propagate our info down
			try {
				propagateToChannel(schema, iterationNode.getInput(), createUtilities);
			} catch (MissingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Could not set up runtime strategy for input channel to node '"
					+ iterationNode.getPactContract().getName() + "'. Missing type information for key field " + 
					ex.getFieldNumber());
			}
		}
		else if (node instanceof WorksetIterationPlanNode) {
			WorksetIterationPlanNode iterationNode = (WorksetIterationPlanNode) node;
			
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
			
			// traverse the step function
			// pass an empty schema to the next workset and the parent schema to the solution set delta
			// these first traversals are schema only
			traverse(iterationNode.getNextWorkSetPlanNode(), new KeySchema(), false);
			traverse(iterationNode.getSolutionSetDeltaPlanNode(), schema, false);
			
			KeySchema wss = (KeySchema) iterationNode.getWorksetPlanNode().postPassHelper;
			KeySchema sss = (KeySchema) iterationNode.getSolutionSetPlanNode().postPassHelper;
			
			if (wss == null) {
				throw new CompilerException("Error in Optimizer Post Pass: Workset schema is null after first traversal of the step function.");
			}
			if (sss == null) {
				throw new CompilerException("Error in Optimizer Post Pass: Solution set schema is null after first traversal of the step function.");
			}
			
			// make the second pass and instantiate the utilities
			traverse(iterationNode.getNextWorkSetPlanNode(), wss, createUtilities);
			traverse(iterationNode.getSolutionSetDeltaPlanNode(), sss, createUtilities);
			
			// add the types from the solution set schema to the iteration's own schema. since
			// the solution set input and the result must have the same schema, this acts as a sanity check.
			try {
				for (Map.Entry<Integer, Class<? extends Key>> entry : sss) {
					Integer pos = entry.getKey();
					schema.addType(pos, entry.getValue());
				}
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Conflicting key type information for field " + ex.getFieldNumber()
					+ " in node '" + iterationNode.getPactContract().getName() + "'. Contradicting types between the " +
					"result of the iteration and the solution set schema: " + ex.getPreviousType().getName() + 
					" and " + ex.getNewType().getName() + 
					". Most probable cause: Invalid constant field annotations.");
			}
			
			// set the serializers and comparators
			if (createUtilities) {
				WorksetIterationNode optNode = iterationNode.getIterationNode();
				iterationNode.setWorksetSerializer(PactRecordSerializerFactory.get());
				iterationNode.setSolutionSetSerializer(PactRecordSerializerFactory.get());
				try {
					iterationNode.setSolutionSetComparator(createComparator(optNode.getSolutionSetKeyFields(), null, sss));
				} catch (MissingFieldTypeInfoException ex) {
					throw new CompilerPostPassException("Could not set up the solution set for workset iteration '" + 
							optNode.getPactContract().getName() + "'. Missing type information for key field " + ex.getFieldNumber() + '.');
				}
			}
			
			// done, we can now propagate our info down
			try {
				propagateToChannel(schema, iterationNode.getInitialSolutionSetInput(), createUtilities);
				propagateToChannel(wss, iterationNode.getInitialWorksetInput(), createUtilities);
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
			
			if (createUtilities) {
				// parameterize the node's driver strategy
				if (sn.getDriverStrategy().requiresComparator()) {
					try {
						sn.setComparator(createComparator(sn.getKeys(), sn.getSortOrders(), schema));
					} catch (MissingFieldTypeInfoException ex) {
						throw new CompilerPostPassException("Could not set up runtime strategy for node '" + 
							contract.getName() + "'. Missing type information for key field " + ex.getFieldNumber());
					}
				}
			}
			
			// done, we can now propagate our info down
			try {
				propagateToChannel(schema, sn.getInput(), createUtilities);
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
			if (createUtilities) {
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
			}
			
			// done, we can now propagate our info down
			try {
				propagateToChannel(schema1, dn.getInput1(), createUtilities);
			} catch (MissingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Could not set up runtime strategy for the first input channel to node '"
					+ contract.getName() + "'. Missing type information for key field " + ex.getFieldNumber());
			}
			try {
				propagateToChannel(schema2, dn.getInput2(), createUtilities);
			} catch (MissingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Could not set up runtime strategy for the second input channel to node '"
					+ contract.getName() + "'. Missing type information for key field " + ex.getFieldNumber());
			}
		}
		else if (node instanceof UnionPlanNode) {
			// only propagate the info down
			try {
				for (Iterator<Channel> channels = node.getInputs(); channels.hasNext(); ) {
					propagateToChannel(parentSchema, channels.next(), createUtilities);
				}
			} catch (MissingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Could not set up runtime strategy for the input channel to " +
						" a union node. Missing type information for key field " + ex.getFieldNumber());
			}
		}
		// catch the sources of the iterative step functions
		else if (node instanceof BulkPartialSolutionPlanNode || 
				 node instanceof SolutionSetPlanNode ||
				 node instanceof WorksetPlanNode)
		{
			// get the nodes current schema
			KeySchema schema;
			String name;
			if (node instanceof BulkPartialSolutionPlanNode) {
				BulkPartialSolutionPlanNode psn = (BulkPartialSolutionPlanNode) node;
				if (psn.postPassHelper == null) {
					schema = new KeySchema();
					psn.postPassHelper = schema;
				} else {
					schema = (KeySchema) psn.postPassHelper;
				}
				name = "partial solution of iteration '" +
					psn.getPartialSolutionNode().getIterationNode().getPactContract().getName() + "'";
			}
			else if (node instanceof SolutionSetPlanNode) {
				SolutionSetPlanNode ssn = (SolutionSetPlanNode) node;
				if (ssn.postPassHelper == null) {
					schema = new KeySchema();
					ssn.postPassHelper = schema;
				} else {
					schema = (KeySchema) ssn.postPassHelper;
				}
				name = "solution set of iteration '" +
						ssn.getSolutionSetNode().getIterationNode().getPactContract().getName() + "'";
			}
			else if (node instanceof WorksetPlanNode) {
				WorksetPlanNode wsn = (WorksetPlanNode) node;
				if (wsn.postPassHelper == null) {
					schema = new KeySchema();
					wsn.postPassHelper = schema;
				} else {
					schema = (KeySchema) wsn.postPassHelper;
				}
				name = "solution set of iteration '" +
						wsn.getWorksetNode().getIterationNode().getPactContract().getName() + "'";
			} else {
				throw new CompilerException();
			}
			
			schema.increaseNumConnectionsThatContributed();
			
			// add the parent schema to the schema
			try {
				for (Map.Entry<Integer, Class<? extends Key>> entry : parentSchema) {
					Integer pos = entry.getKey();
					schema.addType(pos, entry.getValue());
				}
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Conflicting key type information for field " + ex.getFieldNumber()
					+ " in the " + name + ", as propagated from the successor nodes. Conflicting types: " + 
					ex.getPreviousType().getName() + " and " + ex.getNewType().getName() + 
					". Most probable cause: Invalid constant field annotations.");
			}
		}
		else {
			throw new CompilerPostPassException("Unknown node type encountered: " + node.getClass().getName());
		}
	}
	
	private void propagateToChannel(KeySchema schema, Channel channel, boolean createUtilities) throws MissingFieldTypeInfoException {
		if (createUtilities) {
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
		}
		
		// propagate the the channel's source model
		traverse(channel.getSource(), schema, createUtilities);
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
