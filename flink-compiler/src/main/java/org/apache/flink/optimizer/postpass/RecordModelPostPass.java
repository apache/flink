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

import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.RecordOperator;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.base.CoGroupOperatorBase;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.common.typeutils.record.RecordComparatorFactory;
import org.apache.flink.api.common.typeutils.record.RecordPairComparatorFactory;
import org.apache.flink.api.common.typeutils.record.RecordSerializerFactory;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.CompilerPostPassException;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.types.Key;

/**
 * Post pass implementation for the Record data model. Does only type inference and creates
 * serializers and comparators.
 */
public class RecordModelPostPass extends GenericFlatTypePostPass<Class<? extends Key<?>>, SparseKeySchema> {
	
	// --------------------------------------------------------------------------------------------
	//  Type specific methods that extract schema information
	// --------------------------------------------------------------------------------------------
	
	@Override
	protected SparseKeySchema createEmptySchema() {
		return new SparseKeySchema();
	}
	
	@Override
	protected void getSinkSchema(SinkPlanNode sinkPlanNode, SparseKeySchema schema) throws CompilerPostPassException {
		GenericDataSinkBase<?> sink = sinkPlanNode.getSinkNode().getOperator();
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
		SingleInputOperator<?, ?, ?> contract = (SingleInputOperator<?, ?, ?>) node.getSingleInputNode().getOperator();
		if (! (contract instanceof RecordOperator)) {
			throw new CompilerPostPassException("Error: Operator is not a Record based contract. Wrong compiler invokation.");
		}
		RecordOperator recContract = (RecordOperator) contract;
		
		// add the information to the schema
		int[] localPositions = contract.getKeyColumns(0);
		Class<? extends Key<?>>[] types = recContract.getKeyClasses();
		for (int i = 0; i < localPositions.length; i++) {
			schema.addType(localPositions[i], types[i]);
		}
		
		// this is a temporary fix, we should solve this more generic
		if (contract instanceof GroupReduceOperatorBase) {
			Ordering groupOrder = ((GroupReduceOperatorBase<?, ?, ?>) contract).getGroupOrder();
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
		DualInputOperator<?, ?, ?, ?> contract = node.getTwoInputNode().getOperator();
		if (! (contract instanceof RecordOperator)) {
			throw new CompilerPostPassException("Error: Operator is not a Pact Record based contract. Wrong compiler invokation.");
		}
		
		RecordOperator recContract = (RecordOperator) contract;
		int[] localPositions1 = contract.getKeyColumns(0);
		int[] localPositions2 = contract.getKeyColumns(1);
		Class<? extends Key<?>>[] types = recContract.getKeyClasses();
		
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
		if (contract instanceof CoGroupOperatorBase) {
			Ordering groupOrder1 = ((CoGroupOperatorBase<?, ?, ?, ?>) contract).getGroupOrderForInputOne();
			Ordering groupOrder2 = ((CoGroupOperatorBase<?, ?, ?, ?>) contract).getGroupOrderForInputTwo();
			
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
			Class<? extends Key<?>> type = o.getType(i);
			schema.addType(pos, type);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Methods to create serializers and comparators
	// --------------------------------------------------------------------------------------------
	
	@Override
	protected TypeSerializerFactory<?> createSerializer(SparseKeySchema schema) {
		return RecordSerializerFactory.get();
	}
	
	@Override
	protected RecordComparatorFactory createComparator(FieldList fields, boolean[] directions, SparseKeySchema schema)
			throws MissingFieldTypeInfoException
	{
		int[] positions = fields.toArray();
		Class<? extends Key<?>>[] keyTypes = PostPassUtils.getKeys(schema, positions);
		return new RecordComparatorFactory(positions, keyTypes, directions);
	}
	
	@Override
	protected RecordPairComparatorFactory createPairComparator(FieldList fields1, FieldList fields2, boolean[] sortDirections, 
			SparseKeySchema schema1, SparseKeySchema schema2)
	{
		return RecordPairComparatorFactory.get();
	}
}
