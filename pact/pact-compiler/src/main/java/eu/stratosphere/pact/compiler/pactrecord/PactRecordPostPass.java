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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.contract.SingleInputContract;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.compiler.CompilerPostPassException;
import eu.stratosphere.pact.compiler.OptimizerPostPass;
import eu.stratosphere.pact.compiler.plan.SingleInputNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SourcePlanNode;
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
	
	protected void traverse(PlanNode node, KeySchema parentSchema)
	{
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
			propagateToChannel(schema, inchannel);
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
			for (Map.Entry<Integer, Class<? extends Key>> entry : parentSchema) {
				final Integer pos = entry.getKey();
				if (optNode.isFieldConstant(0, pos)) {
					schema.addKeyType(pos, entry.getValue());
				}
			}
			
			// check whether all outgoing channels have not yet contributed. come back later if not.
			if (schema.getNumConnectionsThatContributed() <= sn.getOutgoingChannels().size()) {
				return;
			}
			
			// add the nodes local information. this automatically consistency checks
			final SingleInputContract<?> contract = optNode.getPactContract();
			final int[] localPositions = contract.getKeyColumnNumbers(0);
			final Class<? extends Key>[] types = contract.getKeyClasses();
			for (int i = 0; i < localPositions.length; i++) {
				schema.addKeyType(localPositions[i], types[i]);
			}
			
			// parameterize the node's driver strategy
			sn.setSerializer(PactRecordSerializerFactory.get());
			if (sn.getDriverStrategy().requiresComparator()) {
				sn.setComparator(createComparator(sn.getKeys(), sn.getSortOrders(), schema));
			}
			
			// done, we can now propagate our info down
			propagateToChannel(schema, sn.getInput());
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
	
	// --------------------------------------------------------------------------------------------

	private static final class KeySchema implements Iterable<Map.Entry<Integer, Class<? extends Key>>>
	{
		private final Map<Integer, Class<? extends Key>> schema;
		
		private int numConnectionsThatContributed;
		
		KeySchema() {
			this.schema = new HashMap<Integer, Class<? extends Key>>();
		}
		
		void addKeyType(Integer key, Class<? extends Key> type) throws ConflictingFieldTypeInfoException 
		{
			Class<? extends Key> previous = this.schema.put(key, type);
			if (previous != null && previous != type) {
				throw new ConflictingFieldTypeInfoException(key, previous, type);
			}
		}
		
		Class<? extends Key> getType(Integer field) {
			return this.schema.get(field);
		}
		
		public Iterator<Entry<Integer, Class<? extends Key>>> iterator() {
			return this.schema.entrySet().iterator();
		}
		
		int getNumConnectionsThatContributed() {
			return this.numConnectionsThatContributed;
		}
		
		void increaseNumConnectionsThatContributed() {
			this.numConnectionsThatContributed++;
		}
	}
	
	@SuppressWarnings("serial")
	private static final class MissingFieldTypeInfoException extends Exception
	{
		private final int fieldNumber;

		MissingFieldTypeInfoException(int fieldNumber) {
			this.fieldNumber = fieldNumber;
		}
		
		int getFieldNumber() {
			return fieldNumber;
		}
	}
	
	@SuppressWarnings("serial")
	private static final class ConflictingFieldTypeInfoException extends Exception
	{
		private final int fieldNumber;
		
		private final Class<? extends Key> type1, type2;

		ConflictingFieldTypeInfoException(int fieldNumber, Class<? extends Key> type1, Class<? extends Key> type2) {
			this.fieldNumber = fieldNumber;
			this.type1 = type1;
			this.type2 = type2;
		}
		
		int getFieldNumber() {
			return fieldNumber;
		}

		Class<? extends Key> getType1() {
			return type1;
		}

		Class<? extends Key> getType2() {
			return type2;
		}
	}
}
