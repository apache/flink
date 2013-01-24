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

package eu.stratosphere.pact.array.optimizer;

import java.util.Map;

import eu.stratosphere.pact.array.io.ArrayModelOutputFormat;
import eu.stratosphere.pact.array.stubs.AbstractArrayModelStub;
import eu.stratosphere.pact.array.typeutils.ArrayRecordComparatorFactory;
import eu.stratosphere.pact.array.typeutils.ArrayRecordSerializerFactory;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.CompilerPostPassException;
import eu.stratosphere.pact.compiler.plan.SingleInputNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SourcePlanNode;
import eu.stratosphere.pact.compiler.postpass.ConflictingFieldTypeInfoException;
import eu.stratosphere.pact.compiler.postpass.MissingFieldTypeInfoException;
import eu.stratosphere.pact.compiler.postpass.OptimizerPostPass;
import eu.stratosphere.pact.compiler.postpass.TypeSchema;
import eu.stratosphere.pact.generic.contract.SingleInputContract;
import eu.stratosphere.pact.generic.io.OutputFormat;

/**
 * 
 */
public class ArrayModelPostPass implements OptimizerPostPass
{
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.OptimizerPostPass#postPass(eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan)
	 */
	@Override
	public void postPass(OptimizedPlan plan) {
		for (SinkPlanNode sink : plan.getDataSinks()) {
			traverse(sink, null, 0);
		}
	}
	
	protected void traverse(final PlanNode node, final TypeSchema parentSchema, final int numFields) {
		// distinguish the node types
		if (node instanceof SinkPlanNode) {
			final SinkPlanNode sn = (SinkPlanNode) node;
			
			final Channel inchannel = sn.getInput();
			final TypeSchema schema = new TypeSchema();
			sn.postPassHelper = schema;
			
			final GenericDataSink pactSink = sn.getSinkNode().getPactContract();
			
			// --------------------- add type information from input schema -----------------------
			
			final Class<? extends OutputFormat<?>> format = pactSink.getFormatClass();
			final int numFieldsInInput;
			
			if (ArrayModelOutputFormat.class.isAssignableFrom(format)) {
				final Class<? extends ArrayModelOutputFormat> formatClass = format.asSubclass(ArrayModelOutputFormat.class);
				final ArrayModelOutputFormat formatInstance = InstantiationUtil.instantiate(formatClass, ArrayModelOutputFormat.class);
				final Class<? extends Value>[] types = formatInstance.getDataTypes();
				
				numFieldsInInput = types.length;
				try {
					addToSchema(types, schema);
				} catch (ConflictingFieldTypeInfoException ex) {
					throw new RuntimeException("Bug! Conflict on first set of type entries in the data sink.");
				}
			} else {
				throw new CompilerException("Incompatibe input format type. Array model programs require an ArrayModelInputFormat");
			}
			
			// --------------------- add the type information from the ordering -----------------------
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
			
			// --------------------- recurse -----------------------
			try {
				propagateToChannel(inchannel, schema, numFieldsInInput);
			} catch (MissingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("BUG: Missing type infomation for input to to data sink.");
			}
		}
		else if (node instanceof SourcePlanNode) {
			final SourcePlanNode n = (SourcePlanNode) node;
			try {
				n.setSerializer(createSerializer(parentSchema, numFields));
			} catch (MissingFieldTypeInfoException e) {
				throw new RuntimeException("Bug: Missing field types for data source");
			}
		}
		else if (node instanceof SingleInputPlanNode) {
			final SingleInputPlanNode sn = (SingleInputPlanNode) node;
			
			// get the nodes current schema
			final TypeSchema schema;
			if (sn.postPassHelper == null) {
				schema = new TypeSchema();
				sn.postPassHelper = schema;
			} else {
				schema = (TypeSchema) sn.postPassHelper;
			}
			schema.increaseNumConnectionsThatContributed();
			
			// add the parent schema to the schema
			final SingleInputNode optNode = sn.getSingleInputNode();
			try {
				for (Map.Entry<Integer, Class<? extends Value>> entry : parentSchema) {
					final Integer pos = entry.getKey();
					if (optNode.isFieldConstant(0, pos)) {
						schema.addType(pos, entry.getValue());
					}
				}
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Conflicting type information for field " + ex.getFieldNumber()
					+ " in node '" + optNode.getPactContract().getName() + "' propagated from successor node. " +
					"Conflicting types: " + ex.getPreviousType().getName() + " and " + ex.getNewType().getName() +
					". Most probable cause: Invalid constant field annotations.");
					
			}
			
			// check whether all outgoing channels have not yet contributed. come back later if not.
			if (schema.getNumConnectionsThatContributed() < sn.getOutgoingChannels().size()) {
				return;
			}
			
			// add the nodes local information. this automatically consistency checks
			final SingleInputContract<?> contract = optNode.getPactContract();
			final int numFieldsInInput;
			
			// add the local schema annotation information
			Class<? extends Stub> stubClass = contract.getUserCodeClass();
			if (AbstractArrayModelStub.class.isAssignableFrom(stubClass)) {
				AbstractArrayModelStub ams = (AbstractArrayModelStub) InstantiationUtil.instantiate(stubClass, Stub.class);
				
				final Class<? extends Value>[] types = ams.getDataTypes();
				numFieldsInInput = types.length;
				try {
					addToSchema(types, schema);
				} catch (ConflictingFieldTypeInfoException ex) {
					throw new CompilerPostPassException("Conflicting type information for field " + ex.getFieldNumber()
						+ " in node '" + optNode.getPactContract().getName() + "' between types declared in the node's "
						+ "contract and types inferred from successor contracts. Conflicting types: "
						+ ex.getPreviousType().getName() + " and " + ex.getNewType().getName()
						+ ". Most probable cause: Invalid constant field annotations.");
				}
			} else {
				throw new CompilerException("Incompatibe stub type. Array model programs require array model stubs.");
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
				propagateToChannel(sn.getInput(), schema, numFieldsInInput);
			} catch (MissingFieldTypeInfoException ex) {
				throw new CompilerPostPassException("Could not set up runtime strategy for input channel to node '"
					+ contract.getName() + "'. Missing type information for key field " + ex.getFieldNumber());
			}

		}
//		else if (node instanceof DualInputPlanNode) {
//			final DualInputPlanNode dn = (DualInputPlanNode) node;
//			
//			// get the nodes current schema
//			final TypeSchema schema1;
//			final TypeSchema schema2;
//			if (dn.postPassHelper1 == null) {
//				schema1 = new TypeSchema();
//				schema2 = new TypeSchema();
//				dn.postPassHelper1 = schema1;
//				dn.postPassHelper2 = schema2;
//			} else {
//				schema1 = (TypeSchema) dn.postPassHelper1;
//				schema2 = (TypeSchema) dn.postPassHelper2;
//			}
//
//			schema1.increaseNumConnectionsThatContributed();
//			schema2.increaseNumConnectionsThatContributed();
//			
//			// add the parent schema to the schema
//			final TwoInputNode optNode = dn.getTwoInputNode();
//			try {
//				for (Map.Entry<Integer, Class<? extends Key>> entry : parentSchema) {
//					final Integer pos = entry.getKey();
//					if (optNode.isFieldConstant(0, pos)) {
//						schema1.addKeyType(pos, entry.getValue());
//					}
//					if (optNode.isFieldConstant(1, pos)) {
//						schema2.addKeyType(pos, entry.getValue());
//					}
//				}
//			} catch (ConflictingFieldTypeInfoException ex) {
//				throw new CompilerPostPassException("Conflicting key type information for field " + ex.getFieldNumber()
//					+ " in node '" + optNode.getPactContract().getName() + "' propagated from successor node. " +
//					"Conflicting types: " + ex.getPreviousType().getName() + " and " + ex.getNewType().getName() +
//					". Most probably cause: Invalid constant field annotations.");
//					
//			}
//			
//			// check whether all outgoing channels have not yet contributed. come back later if not.
//			if (schema1.getNumConnectionsThatContributed() < dn.getOutgoingChannels().size()) {
//				return;
//			}
//			
//			// add the nodes local information. this automatically consistency checks
//			final DualInputContract<?> contract = optNode.getPactContract();
//			if (! (contract instanceof RecordContract)) {
//				throw new CompilerPostPassException("Error: Contract is not a Pact Record based contract. Wrong compiler invokation.");
//			}
//			final RecordContract recContract = (RecordContract) contract;
//			final int[] localPositions1 = contract.getKeyColumns(0);
//			final int[] localPositions2 = contract.getKeyColumns(1);
//			final Class<? extends Key>[] types = recContract.getKeyClasses();
//			
//			if (localPositions1.length != localPositions2.length) {
//				throw new CompilerException("Error: The keys for the first and second input have a different number of fields.");
//			}
//			
//			try {
//				for (int i = 0; i < localPositions1.length; i++) {
//					schema1.addKeyType(localPositions1[i], types[i]);
//				}
//			} catch (ConflictingFieldTypeInfoException ex) {
//				throw new CompilerPostPassException("Conflicting type information for field " + ex.getFieldNumber()
//					+ " in the first input of node '" + optNode.getPactContract().getName() + 
//					"' between types declared in the node's contract and types inferred from successor contracts. " +
//					"Conflicting types: " + ex.getPreviousType().getName() + " and " + ex.getNewType().getName()
//					+ ". Most probable cause: Invalid constant field annotations.");
//			}
//			try {
//				for (int i = 0; i < localPositions2.length; i++) {
//					schema2.addKeyType(localPositions2[i], types[i]);
//				}
//			} catch (ConflictingFieldTypeInfoException ex) {
//				throw new CompilerPostPassException("Conflicting type information for field " + ex.getFieldNumber()
//					+ " in the second input of node '" + optNode.getPactContract().getName() + 
//					"' between types declared in the node's contract and types inferred from successor contracts. " +
//					"Conflicting types: " + ex.getPreviousType().getName() + " and " + ex.getNewType().getName()
//					+ ". Most probable cause: Invalid constant field annotations.");
//			}
//			
//			// parameterize the node's driver strategy
//			if (dn.getDriverStrategy().requiresComparator()) {
//				// set the individual comparators
//				try {
//					dn.setComparator1(createComparator(dn.getKeysForInput1(), dn.getSortOrders(), schema1));
//					dn.setComparator2(createComparator(dn.getKeysForInput2(), dn.getSortOrders(), schema2));
//				} catch (MissingFieldTypeInfoException ex) {
//					throw new CompilerPostPassException("Could not set up runtime strategy for node '" + 
//						contract.getName() + "'. Missing type information for key field " + ex.getFieldNumber());
//				}
//				
//				// set the pair comparator
//				dn.setPairComparator(ArrayRecordPairComparatorFactory.get());
//			}
//			
//			// done, we can now propagate our info down
//			try {
//				propagateToChannel(schema1, dn.getInput1());
//			} catch (MissingFieldTypeInfoException ex) {
//				throw new CompilerPostPassException("Could not set up runtime strategy for the first input channel to node '"
//					+ contract.getName() + "'. Missing type information for field " + ex.getFieldNumber());
//			}
//			try {
//				propagateToChannel(schema2, dn.getInput2());
//			} catch (MissingFieldTypeInfoException ex) {
//				throw new CompilerPostPassException("Could not set up runtime strategy for the second input channel to node '"
//					+ contract.getName() + "'. Missing type information for field " + ex.getFieldNumber());
//			}
//		}
		else {
			throw new CompilerPostPassException("Unknown node type encountered: " + node.getClass().getName());
		}
	}
	
	private void propagateToChannel(Channel channel, TypeSchema schema, int numFields) 
			throws MissingFieldTypeInfoException
	{
		// the serializer is always the same and always exists
		channel.setSerializer(createSerializer(schema, numFields));
		
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
		traverse(channel.getSource(), schema, numFields);
	}
	
	private void addOrderingToSchema(Ordering o, TypeSchema schema) throws ConflictingFieldTypeInfoException {
		for (int i = 0; i < o.getNumberOfFields(); i++) {
			Integer pos = o.getFieldNumber(i);
			Class<? extends Key> type = o.getType(i);
			schema.addType(pos, type);
		}
	}
	
	private void addToSchema(Class<? extends Value>[] types, TypeSchema schema) throws ConflictingFieldTypeInfoException {
		for (int i = 0; i < types.length; i++) {
			schema.addType(i, types[i]);
		}
	}
	
	private ArrayRecordSerializerFactory createSerializer(TypeSchema schema, int numFields)
		throws MissingFieldTypeInfoException
	{
		@SuppressWarnings("unchecked")
		final Class<? extends Value>[] types = new Class[numFields];
		for (int i = 0; i < numFields; i++) {
			Class<? extends Value> type = schema.getType(i);
			if (type == null) {
				throw new MissingFieldTypeInfoException(i);
			} else {
				types[i] = type;
			}
		}
		return new ArrayRecordSerializerFactory(types);
	}
	
	private ArrayRecordComparatorFactory createComparator(FieldList fields, boolean[] directions, TypeSchema schema)
			throws MissingFieldTypeInfoException
	{
		final int[] positions = fields.toArray();
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyTypes = new Class[fields.size()];
		
		for (int i = 0; i < fields.size(); i++) {
			Class<? extends Value> type = schema.getType(positions[i]);
			if (!Key.class.isAssignableFrom(type)) {
				throw new CompilerException("The field type " + type.getName() +
					" cannot be used as a key because it does not implement the interface 'Key'");
			}
			@SuppressWarnings("unchecked")
			Class<? extends Key> keyType = (Class<? extends Key>) type;
			if (type == null) {
				throw new MissingFieldTypeInfoException(i);
			} else {
				keyTypes[i] = keyType;
			}
		}
		return new ArrayRecordComparatorFactory(positions, keyTypes, directions);
	}
}
