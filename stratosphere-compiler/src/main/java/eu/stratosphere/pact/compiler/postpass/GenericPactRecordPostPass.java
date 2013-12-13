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

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.contract.RecordContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.CompilerPostPassException;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.generic.contract.DualInputContract;
import eu.stratosphere.pact.generic.contract.SingleInputContract;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializerFactory;

/**
 * Post pass implementation for the PactRecord data model. Does only type inference and creates
 * serializers and comparators.
 */
public class GenericPactRecordPostPass extends GenericRecordPostPass<Class<? extends Key>, SparseKeySchema> {
	
	// --------------------------------------------------------------------------------------------
	//  Type specific methods that extract schema information
	// --------------------------------------------------------------------------------------------
	
	@Override
	protected SparseKeySchema createEmptySchema() {
		return new SparseKeySchema();
	}
	
	@Override
	protected void getSinkSchema(SinkPlanNode sinkPlanNode, SparseKeySchema schema) throws CompilerPostPassException {
		GenericDataSink sink = sinkPlanNode.getSinkNode().getPactContract();
		Ordering partitioning = sink.getPartitionOrdering();
		Ordering sorting = sink.getLocalOrder();
		
		try {
			if (partitioning != null) {
				addOrderingToSchema(partitioning, schema);
			}
			if (sorting != null) {
				addOrderingToSchema(sorting, schema);
			}
		} catch (ConflictingFieldTypeInfoException ex) {
			throw new CompilerPostPassException("Conflicting information found when adding data sink types. " +
					"Probable reason is contradicting type infos for partitioning and sorting ordering.");
		}
	}
	
	@Override
	protected void getSingleInputNodeSchema(SingleInputPlanNode node, SparseKeySchema schema)
			throws CompilerPostPassException, ConflictingFieldTypeInfoException
	{
		// check that we got the right types
		SingleInputContract<?> contract = (SingleInputContract<?>) node.getSingleInputNode().getPactContract();
		if (! (contract instanceof RecordContract)) {
			throw new CompilerPostPassException("Error: Contract is not a PactRecord based contract. Wrong compiler invokation.");
		}
		RecordContract recContract = (RecordContract) contract;
		
		// add the information to the schema
		int[] localPositions = contract.getKeyColumns(0);
		Class<? extends Key>[] types = recContract.getKeyClasses();
		for (int i = 0; i < localPositions.length; i++) {
			schema.addType(localPositions[i], types[i]);
		}
		
		// this is a temporary fix, we should solve this more generic
		if (contract instanceof ReduceContract) {
			Ordering groupOrder = ((ReduceContract) contract).getGroupOrder();
			if (groupOrder != null) {
				addOrderingToSchema(groupOrder, schema);
			}
		}
	}
	
	@Override
	protected void getDualInputNodeSchema(DualInputPlanNode node, SparseKeySchema input1Schema, SparseKeySchema input2Schema)
			throws CompilerPostPassException, ConflictingFieldTypeInfoException
	{
		// add the nodes local information. this automatically consistency checks
		DualInputContract<?> contract = node.getTwoInputNode().getPactContract();
		if (! (contract instanceof RecordContract)) {
			throw new CompilerPostPassException("Error: Contract is not a Pact Record based contract. Wrong compiler invokation.");
		}
		
		RecordContract recContract = (RecordContract) contract;
		int[] localPositions1 = contract.getKeyColumns(0);
		int[] localPositions2 = contract.getKeyColumns(1);
		Class<? extends Key>[] types = recContract.getKeyClasses();
		
		if (localPositions1.length != localPositions2.length) {
			throw new CompilerException("Error: The keys for the first and second input have a different number of fields.");
		}
		
		for (int i = 0; i < localPositions1.length; i++) {
			input1Schema.addType(localPositions1[i], types[i]);
		}
		for (int i = 0; i < localPositions2.length; i++) {
			input2Schema.addType(localPositions2[i], types[i]);
		}
		
		
		// this is a temporary fix, we should solve this more generic
		if (contract instanceof CoGroupContract) {
			Ordering groupOrder1 = ((CoGroupContract) contract).getGroupOrderForInputOne();
			Ordering groupOrder2 = ((CoGroupContract) contract).getGroupOrderForInputTwo();
			
			if (groupOrder1 != null) {
				addOrderingToSchema(groupOrder1, input1Schema);
			}
			if (groupOrder2 != null) {
				addOrderingToSchema(groupOrder2, input2Schema);
			}
		}
	}

	private void addOrderingToSchema(Ordering o, SparseKeySchema schema) throws ConflictingFieldTypeInfoException {
		for (int i = 0; i < o.getNumberOfFields(); i++) {
			Integer pos = o.getFieldNumber(i);
			Class<? extends Key> type = o.getType(i);
			schema.addType(pos, type);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Methods to create serializers and comparators
	// --------------------------------------------------------------------------------------------
	
	@Override
	protected TypeSerializerFactory<?> createSerializer(SparseKeySchema schema) {
		return PactRecordSerializerFactory.get();
	}
	
	@Override
	protected PactRecordComparatorFactory createComparator(FieldList fields, boolean[] directions, SparseKeySchema schema)
			throws MissingFieldTypeInfoException
	{
		int[] positions = fields.toArray();
		Class<? extends Key>[] keyTypes = PostPassUtils.getKeys(schema, positions);
		return new PactRecordComparatorFactory(positions, keyTypes, directions);
	}
	
	@Override
	protected PactRecordPairComparatorFactory createPairComparator(FieldList fields1, FieldList fields2, boolean[] sortDirections, 
			SparseKeySchema schema1, SparseKeySchema schema2)
	{
		return PactRecordPairComparatorFactory.get();
	}
}
