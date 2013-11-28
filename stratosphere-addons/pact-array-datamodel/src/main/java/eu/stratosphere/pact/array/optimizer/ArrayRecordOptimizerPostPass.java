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

import eu.stratosphere.pact.array.io.ArrayModelOutputFormat;
import eu.stratosphere.pact.array.stubs.AbstractArrayModelStub;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.CompilerPostPassException;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.compiler.postpass.ConflictingFieldTypeInfoException;
import eu.stratosphere.pact.compiler.postpass.DenseValueSchema;
import eu.stratosphere.pact.compiler.postpass.GenericRecordPostPass;
import eu.stratosphere.pact.compiler.postpass.MissingFieldTypeInfoException;
import eu.stratosphere.pact.compiler.postpass.PostPassUtils;
import eu.stratosphere.pact.generic.contract.DualInputContract;
import eu.stratosphere.pact.generic.contract.SingleInputContract;
import eu.stratosphere.pact.generic.io.OutputFormat;
import eu.stratosphere.pact.runtime.plugable.arrayrecord.ArrayRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.arrayrecord.ArrayRecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.arrayrecord.ArrayRecordSerializerFactory;

/**
 * Post pass implementation for the array record data model. Does only type inference and creates
 * serializers and comparators.
 */
public class ArrayRecordOptimizerPostPass extends GenericRecordPostPass<Class<? extends Value>, DenseValueSchema> {

	// --------------------------------------------------------------------------------------------
	//  Type specific methods that extract schema information
	// ------------------------------------------------------------------------------------------
	
	@Override
	protected DenseValueSchema createEmptySchema() {
		return new DenseValueSchema();
	}
	
	@Override
	protected void getSinkSchema(SinkPlanNode sinkPlanNode, DenseValueSchema schema) throws CompilerPostPassException {
		GenericDataSink sink = sinkPlanNode.getSinkNode().getPactContract();
		OutputFormat<?> format = sink.getFormatWrapper().getUserCodeObject();
		
		if (ArrayModelOutputFormat.class.isAssignableFrom(format.getClass())) {
			ArrayModelOutputFormat formatInstance = (ArrayModelOutputFormat) format;
			Class<? extends Value>[] types = formatInstance.getDataTypes();
			
			try {
				addToSchema(types, schema);
			} catch (ConflictingFieldTypeInfoException ex) {
				throw new RuntimeException("Bug! Conflict on first set of type entries in the data sink.");
			}
			
			// add the type information from the ordering 
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
				throw new CompilerPostPassException("Conflicting information found when adding data sink types.");
			}
			
			schema.setNumFields(types.length);
		} else {
			throw new CompilerException("Incompatibe input format type. Array model programs require an " + 
					ArrayModelOutputFormat.class.getName());
		}
	}
	
	@Override
	protected void getSingleInputNodeSchema(SingleInputPlanNode node, DenseValueSchema schema)
			throws CompilerPostPassException, ConflictingFieldTypeInfoException
	{
		SingleInputContract<?> contract = (SingleInputContract<?>) node.getSingleInputNode().getPactContract();
		Stub stub = contract.getUserCodeWrapper().getUserCodeObject();
		
		if (AbstractArrayModelStub.class.isAssignableFrom(stub.getClass())) {
			AbstractArrayModelStub ams = (AbstractArrayModelStub) stub;
			Class<? extends Value>[] types = ams.getDataTypes(0);
			
			if (types == null) {
				throw new CompilerPostPassException("Missing type annotation in UDF for '" + contract.getName() + "'.");
			}
			
			addToSchema(types, schema);
			schema.setNumFields(types.length);
		} else {
			throw new CompilerException("Incompatibe stub type. Array data model programs require array data model stubs.");
		}
	}
	
	@Override
	protected void getDualInputNodeSchema(DualInputPlanNode node, DenseValueSchema input1Schema, DenseValueSchema input2Schema)
			throws CompilerPostPassException, ConflictingFieldTypeInfoException
	{
		// add the nodes local information. this automatically consistency checks
		DualInputContract<?> contract = node.getTwoInputNode().getPactContract();
		Stub stub = contract.getUserCodeWrapper().getUserCodeObject();
		
		if (AbstractArrayModelStub.class.isAssignableFrom(stub.getClass())) {
			AbstractArrayModelStub ams = (AbstractArrayModelStub) stub;
			
			Class<? extends Value>[] types1 = ams.getDataTypes(0);
			Class<? extends Value>[] types2 = ams.getDataTypes(1);
			
			if (types1 == null) {
				throw new CompilerPostPassException("Missing type annotation for first parameter type in UDF for '" + contract.getName() + "'.");
			}
			if (types2 == null) {
				throw new CompilerPostPassException("Missing type annotation for second parameter type in UDF for '" + contract.getName() + "'.");
			}
			
			addToSchema(types1, input1Schema);
			addToSchema(types2, input2Schema);
			
			input1Schema.setNumFields(types1.length);
			input2Schema.setNumFields(types2.length);
		} else {
			throw new CompilerException("Incompatibe stub type. Array data model programs require array data model stubs.");
		}
		
	}
	// --------------------------------------------------------------------------------------------
	//  Methods to create serializers and comparators
	// --------------------------------------------------------------------------------------------
	
	@Override
	protected ArrayRecordSerializerFactory createSerializer(DenseValueSchema schema) throws MissingFieldTypeInfoException {
		final int numFields = schema.getNumFields();
		if (numFields <= 0) {
			throw new IllegalArgumentException("Bug: Attempt to create serializer for " + numFields + " fields.");
		}
		
		@SuppressWarnings("unchecked")
		Class<? extends Value>[] types = new Class[numFields];
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
	
	@Override
	protected ArrayRecordComparatorFactory createComparator(FieldList fields, boolean[] directions, DenseValueSchema schema)
			throws MissingFieldTypeInfoException
	{
		int[] positions = fields.toArray();
		Class<? extends Key>[] keyTypes = PostPassUtils.getKeys(schema, positions);
		return new ArrayRecordComparatorFactory(positions, keyTypes, directions);
	}
	
	@Override
	protected ArrayRecordPairComparatorFactory createPairComparator(FieldList fields1, FieldList fields2, boolean[] sortDirections, 
		DenseValueSchema schema1, DenseValueSchema schema2)
	{
		return ArrayRecordPairComparatorFactory.get();
	}
	
	// --------------------------------------------------------------------------------------------
	// Miscellaneous Utilities
	// --------------------------------------------------------------------------------------------
	
	private void addOrderingToSchema(Ordering o, DenseValueSchema schema) throws ConflictingFieldTypeInfoException {
		for (int i = 0; i < o.getNumberOfFields(); i++) {
			Integer pos = o.getFieldNumber(i);
			Class<? extends Key> type = o.getType(i);
			schema.addType(pos, type);
		}
	}
	
	private void addToSchema(Class<? extends Value>[] types, DenseValueSchema schema) throws ConflictingFieldTypeInfoException {
		for (int i = 0; i < types.length; i++) {
			schema.addType(i, types[i]);
		}
	}
	
	

}
