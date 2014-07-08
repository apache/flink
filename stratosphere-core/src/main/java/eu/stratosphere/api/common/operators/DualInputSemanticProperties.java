/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.api.common.operators;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.api.common.operators.util.FieldSet;

/**
 * Container for the semantic properties associated to a dual input operator.
 */
public class DualInputSemanticProperties extends SemanticProperties {
	private static final long serialVersionUID = 1L;
	
	/**
	 * Mapping from fields in the source record(s) in the first input to fields
	 * in the destination record(s).  
	 */
	private Map<Integer,FieldSet> forwardedFields1;
	
	/**
	 * Mapping from fields in the source record(s) in the second input to fields
	 * in the destination record(s).  
	 */
	private Map<Integer,FieldSet> forwardedFields2;
	
	/**
	 * Set of fields that are read in the source record(s) from the
	 * first input.
	 */
	private FieldSet readFields1;

	/**
	 * Set of fields that are read in the source record(s) from the
	 * second input.
	 */
	private FieldSet readFields2;

	
	public DualInputSemanticProperties() {
		super();
		this.init();
	}
	
	/**
	 * Adds, to the existing information, a field that is forwarded directly
	 * from the source record(s) in the first input to the destination
	 * record(s).
	 * 
	 * @param sourceField the position in the source record(s) from the first input
	 * @param destinationField the position in the destination record(s)
	 */
	public void addForwardedField1(int sourceField, int destinationField) {
		FieldSet old = this.forwardedFields1.get(sourceField);
		if (old == null) {
			old = FieldSet.EMPTY_SET;
		}
		
		FieldSet fs = old.addField(destinationField);
		this.forwardedFields1.put(sourceField, fs);
	}
	
	/**
	 * Adds, to the existing information, a field that is forwarded directly
	 * from the source record(s) in the first input to multiple fields in
	 * the destination record(s).
	 * 
	 * @param sourceField the position in the source record(s)
	 * @param destinationFields the position in the destination record(s)
	 */
	public void addForwardedField1(int sourceField, FieldSet destinationFields) {
		FieldSet old = this.forwardedFields1.get(sourceField);
		if (old == null) {
			old = FieldSet.EMPTY_SET;
		}
		
		FieldSet fs = old.addFields(destinationFields);
		this.forwardedFields1.put(sourceField, fs);
	}
	
	/**
	 * Sets a field that is forwarded directly from the source
	 * record(s) in the first input to multiple fields in the
	 * destination record(s).
	 * 
	 * @param sourceField the position in the source record(s)
	 * @param destinationFields the position in the destination record(s)
	 */
	public void setForwardedField1(int sourceField, FieldSet destinationFields) {
		this.forwardedFields1.put(sourceField, destinationFields);
	}
	
	/**
	 * Gets the fields in the destination record where the source
	 * field from the first input is forwarded.
	 * 
	 * @param sourceField the position in the source record
	 * @return the destination fields, or null if they do not exist
	 */
	public FieldSet getForwardedField1(int sourceField) {
		return this.forwardedFields1.get(sourceField);
	}
	
	/**
	 * Adds, to the existing information, a field that is forwarded directly
	 * from the source record(s) in the second input to the destination
	 * record(s).
	 * 
	 * @param sourceField the position in the source record(s) from the first input
	 * @param destinationField the position in the destination record(s)
	 */
	public void addForwardedField2(int sourceField, int destinationField) {
		FieldSet old = this.forwardedFields2.get(sourceField);
		if (old == null) {
			old = FieldSet.EMPTY_SET;
		}
		
		FieldSet fs = old.addField(destinationField);
		this.forwardedFields2.put(sourceField, fs);
	}
	
	/**
	 * Adds, to the existing information, a field that is forwarded directly
	 * from the source record(s) in the second input to multiple fields in
	 * the destination record(s).
	 * 
	 * @param sourceField the position in the source record(s)
	 * @param destinationFields the position in the destination record(s)
	 */
	public void addForwardedField2(int sourceField, FieldSet destinationFields) {
		FieldSet old = this.forwardedFields2.get(sourceField);
		if (old == null) {
			old = FieldSet.EMPTY_SET;
		}
		
		FieldSet fs = old.addFields(destinationFields);
		this.forwardedFields2.put(sourceField, fs);
	}
	
	/**
	 * Sets a field that is forwarded directly from the source
	 * record(s) in the second input to multiple fields in the
	 * destination record(s).
	 * 
	 * @param sourceField the position in the source record(s)
	 * @param destinationFields the position in the destination record(s)
	 */
	public void setForwardedField2(int sourceField, FieldSet destinationFields) {
		this.forwardedFields2.put(sourceField, destinationFields);
	}
	
	/**
	 * Gets the fields in the destination record where the source
	 * field from the second input is forwarded.
	 * 
	 * @param sourceField the position in the source record
	 * @return the destination fields, or null if they do not exist
	 */
	public FieldSet getForwardedField2(int sourceField) {
		return this.forwardedFields2.get(sourceField);
	}
	
	/**
	 * Adds, to the existing information, field(s) that are read in
	 * the source record(s) from the first input.
	 * 
	 * @param readFields the position(s) in the source record(s)
	 */
	public void addReadFields1(FieldSet readFields) {
		if (this.readFields1 == null) {
			this.readFields1 = readFields;
		} else {
			this.readFields1 = this.readFields2.addFields(readFields);
		}
	}
	
	/**
	 * Sets the field(s) that are read in the source record(s) from the first
	 * input.
	 * 
	 * @param readFields the position(s) in the source record(s)
	 */
	public void setReadFields1(FieldSet readFields) {
		this.readFields1 = readFields;
	}
	
	/**
	 * Gets the field(s) in the source record(s) from the first input
	 * that are read.
	 * 
	 * @return the field(s) in the record, or null if they are not set
	 */
	public FieldSet getReadFields1() {
		return this.readFields1;
	}
	
	/**
	 * Adds, to the existing information, field(s) that are read in
	 * the source record(s) from the second input.
	 * 
	 * @param readFields the position(s) in the source record(s)
	 */
	public void addReadFields2(FieldSet readFields) {
		if (this.readFields2 == null) {
			this.readFields2 = readFields;
		} else {
			this.readFields2 = this.readFields2.addFields(readFields);
		}
	}
	
	/**
	 * Sets the field(s) that are read in the source record(s) from the second
	 * input.
	 * 
	 * @param readFields the position(s) in the source record(s)
	 */
	public void setReadFields2(FieldSet readFields) {
		this.readFields2 = readFields;
	}
	
	/**
	 * Gets the field(s) in the source record(s) from the second input
	 * that are read.
	 * 
	 * @return the field(s) in the record, or null if they are not set
	 */
	public FieldSet getReadFields2() {
		return this.readFields2;
	}
	
	/**
	 * Clears the object.
	 */
	@Override
	public void clearProperties() {
		this.init();
		super.clearProperties();
	}
	
	private void init() {
		this.forwardedFields1 = new HashMap<Integer,FieldSet>();
		this.forwardedFields2 = new HashMap<Integer,FieldSet>();
		this.readFields1 = null;
		this.readFields2 = null;
	}
		
}
