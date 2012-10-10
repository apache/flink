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

package eu.stratosphere.pact.compiler.pactrecord;

import java.util.Map;

import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.contract.RecordContract;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.compiler.CompilerPostPassException;
import eu.stratosphere.pact.compiler.plan.SingleInputNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SourcePlanNode;
import eu.stratosphere.pact.compiler.postpass.ConflictingFieldTypeInfoException;
import eu.stratosphere.pact.compiler.postpass.KeySchema;
import eu.stratosphere.pact.compiler.postpass.MissingFieldTypeInfoException;
import eu.stratosphere.pact.compiler.postpass.OptimizerPostPass;
import eu.stratosphere.pact.generic.contract.SingleInputContract;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializerFactory;

/**
 * @author Stephan Ewen
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
			sn.setSerializer(PactRecordSerializerFactory.get());
			
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
						schema.addKeyType(pos, entry.getValue());
					}
				}
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Conflicting key type information for field " + ex.getFieldNumber()
					+ " in node '" + optNode.getPactContract().getName() + "' propagated from successor node. " +
					"Conflicting types: " + ex.getPreviousType().getName() + " and " + ex.getNewType().getName() +
					". Most probably cause: Invalid constant field annotations.");
					
			}
			
			// check whether all outgoing channels have not yet contributed. come back later if not.
			if (schema.getNumConnectionsThatContributed() <= sn.getOutgoingChannels().size()) {
				return;
			}
			
			// add the nodes local information. this automatically consistency checks
			final SingleInputContract<?> contract = optNode.getPactContract();
			if (! (contract instanceof RecordContract)) {
				throw new CompilerPostPassException("Error: Contract is not a Pact Record based contract. Wrong compiler invokation.");
			}
			final RecordContract recContract = (RecordContract) contract;
			final int[] localPositions = contract.getKeyColumns(0);
			final Class<? extends Key>[] types = recContract.getKeyClasses();
			try {
				for (int i = 0; i < localPositions.length; i++) {
					schema.addKeyType(localPositions[i], types[i]);
				}
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Conflicting key type information for field " + ex.getFieldNumber()
					+ " in node '" + optNode.getPactContract().getName() + "' between types declared in the node's "
					+ "contract and types inferred from successor contracts. Conflicting types: "
					+ ex.getPreviousType().getName() + " and " + ex.getNewType().getName()
					+ ". Most probably cause: Invalid constant field annotations.");
					
			}
			
			// parameterize the node's driver strategy
			sn.setSerializer(PactRecordSerializerFactory.get());
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
		else if (node instanceof SourcePlanNode) {
			// nothing to be done here. the source has no input and no strategy itself
		}
		else {
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
			schema.addKeyType(pos, type);
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
