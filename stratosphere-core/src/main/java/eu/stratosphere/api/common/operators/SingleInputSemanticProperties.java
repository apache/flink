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
 * Container for the semantic properties associated to a single input operator.
 */
public class SingleInputSemanticProperties extends SemanticProperties {
	private static final long serialVersionUID = 1L;
	
	/**
	 * Mapping from fields in the source record(s) to fields in the destination
	 * record(s).  
	 */
	private Map<Integer,FieldSet> forwardedFields;
	
	/**
	 * Set of fields that are read in the source record(s).
	 */
	private FieldSet readFields;

	
	public SingleInputSemanticProperties() {
		super();
		this.init();
	}
	
	/**
	 * Adds, to the existing information, a field that is forwarded directly
	 * from the source record(s) to the destination record(s).
	 * 
	 * @param sourceField the position in the source record(s)
	 * @param destinationField the position in the destination record(s)
	 */
	public void addForwardedField(int sourceField, int destinationField) {
		FieldSet fs;
		if((fs = this.forwardedFields.get(sourceField)) != null)
			fs.add(destinationField);
		else {
			fs = new FieldSet(destinationField);
			this.forwardedFields.put(sourceField, fs);
		}
	}
	
	/**
	 * Adds, to the existing information, a field that is forwarded directly
	 * from the source record(s) to multiple fields in the destination
	 * record(s).
	 * 
	 * @param sourceField the position in the source record(s)
	 * @param destinationFields the position in the destination record(s)
	 */
	public void addForwardedField(int sourceField, FieldSet destinationFields) {
		FieldSet fs;
		if((fs = this.forwardedFields.get(sourceField)) != null)
			fs.addAll(destinationFields);
		else {
			fs = new FieldSet(destinationFields);
			this.forwardedFields.put(sourceField, fs);
		}
	}
	
	/**
	 * Sets a field that is forwarded directly from the source
	 * record(s) to multiple fields in the destination record(s).
	 * 
	 * @param sourceField the position in the source record(s)
	 * @param destinationFields the position in the destination record(s)
	 */
	public void setForwardedField(int sourceField, FieldSet destinationFields) {
		this.forwardedFields.put(sourceField,destinationFields);
	}
	
	/**
	 * Gets the fields in the destination record where the source
	 * field is forwarded.
	 * 
	 * @param sourceField the position in the source record
	 * @return the destination fields, or null if they do not exist
	 */
	public FieldSet getForwardedField(int sourceField) {
		return this.forwardedFields.get(sourceField);
	}
	
	/**
	 * Adds, to the existing information, field(s) that are read in
	 * the source record(s).
	 * 
	 * @param readFields the position(s) in the source record(s)
	 */
	public void addReadFields(FieldSet readFields) {
		if(this.readFields == null)
			this.readFields = new FieldSet(readFields);
		else
			this.readFields.addAll(readFields);
	}
	
	/**
	 * Sets the field(s) that are read in the source record(s).
	 * 
	 * @param readFields the position(s) in the source record(s)
	 */
	public void setReadFields(FieldSet readFields) {
		this.readFields = readFields;
	}
	
	/**
	 * Gets the field(s) in the source record(s) that are read.
	 * 
	 * @return the field(s) in the record, or null if they are not set
	 */
	public FieldSet getReadFields() {
		return this.readFields;
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
		this.forwardedFields = new HashMap<Integer,FieldSet>();
		this.readFields = null;
	}
		
}
