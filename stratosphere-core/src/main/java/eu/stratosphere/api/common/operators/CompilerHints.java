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

import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.api.common.operators.util.FieldSet;

/**
 * A class encapsulating compiler hints describing the behavior of the user function.
 * If set, the optimizer will use them to estimate the sizes of the intermediate results.
 * Not that these values are optional hints, the optimizer will always generate a valid plan without
 * them as well. The hints may help, however, to improve the plan choice.
 */
public class CompilerHints {

	private long outputSize = -1;
	
	private long outputCardinality = -1;
	
	private float avgBytesPerOutputRecord = -1.0f; 
	
	private float filterFactor = -1.0f;

	private Set<FieldSet> uniqueFields;

	// --------------------------------------------------------------------------------------------
	//  Basic Record Statistics
	// --------------------------------------------------------------------------------------------
	
	public long getOutputSize() {
		return outputSize;
	}
	
	public void setOutputSize(long outputSize) {
		if (outputSize < 0)
			throw new IllegalArgumentException("The output size cannot be smaller than zero.");
		
		this.outputSize = outputSize;
	}
	
	public long getOutputCardinality() {
		return this.outputCardinality;
	}
	
	public void setOutputCardinality(long outputCardinality) {
		if (outputCardinality < 0)
			throw new IllegalArgumentException("The output cardinality cannot be smaller than zero.");
		
		this.outputCardinality = outputCardinality;
	}
	
	public float getAvgBytesPerOutputRecord() {
		return this.avgBytesPerOutputRecord;
	}
	
	public void setAvgBytesPerOutputRecord(float avgBytesPerOutputRecord) {
		if (avgBytesPerOutputRecord <= 0)
			throw new IllegalArgumentException("The size of produced records must be positive.");
		
		this.avgBytesPerOutputRecord = avgBytesPerOutputRecord;
	}
	
	public float getFilterFactor() {
		return filterFactor;
	}

	
	public void setFilterFactor(float filterFactor) {
		if (filterFactor < 0)
			throw new IllegalArgumentException("The filter factor cannot be smaller than zero.");
		
		this.filterFactor = filterFactor;
	}

	
	// --------------------------------------------------------------------------------------------
	//  Uniqueness
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the FieldSets that are unique
	 * 
	 * @return List of FieldSet that are unique
	 */
	public Set<FieldSet> getUniqueFields() {
		return this.uniqueFields;
	}
	
	/**
	 * Adds a FieldSet to be unique
	 * 
	 * @param uniqueFieldSet The unique FieldSet
	 */
	public void addUniqueField(FieldSet uniqueFieldSet) {
		if (this.uniqueFields == null) {
			this.uniqueFields = new HashSet<FieldSet>();
		}
		this.uniqueFields.add(uniqueFieldSet);
	}
	
	/**
	 * Adds a field as having only unique values.
	 * 
	 * @param field The field with unique values.
	 */
	public void addUniqueField(int field) {
		if (this.uniqueFields == null) {
			this.uniqueFields = new HashSet<FieldSet>();
		}
		this.uniqueFields.add(new FieldSet(field));
	}
	
	/**
	 * Adds multiple FieldSets to be unique
	 * 
	 * @param uniqueFieldSets A set of unique FieldSet
	 */
	public void addUniqueFields(Set<FieldSet> uniqueFieldSets) {
		if (this.uniqueFields == null) {
			this.uniqueFields = new HashSet<FieldSet>();
		}
		this.uniqueFields.addAll(uniqueFieldSets);
	}
	
	public void clearUniqueFields() {
		this.uniqueFields = null;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	protected void copyFrom(CompilerHints source) {
		this.outputSize = source.outputSize;
		this.outputCardinality = source.outputCardinality;
		this.avgBytesPerOutputRecord = source.avgBytesPerOutputRecord;
		this.filterFactor = source.filterFactor;
		
		if (source.uniqueFields != null && source.uniqueFields.size() > 0) {
			if (this.uniqueFields == null) {
				this.uniqueFields = new HashSet<FieldSet>();
			} else {
				this.uniqueFields.clear();
			}
			
			this.uniqueFields.addAll(source.uniqueFields);
		}
	}
}
