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

import java.io.Serializable;

import eu.stratosphere.api.common.operators.util.FieldSet;

/**
 * Container for the semantic properties associated to an operator.
 */
public abstract class SemanticProperties implements Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * Set of fields that are written in the destination record(s).
	 */
	private FieldSet writtenFields;
	
	/**
	 * Adds, to the existing information, field(s) that are written in
	 * the destination record(s).
	 * 
	 * @param writtenFields the position(s) in the destination record(s)
	 */
	public void addWrittenFields(FieldSet writtenFields) {
		if(this.writtenFields == null) {
			this.writtenFields = new FieldSet(writtenFields);
		} else {
			this.writtenFields.addAll(writtenFields);
		}
	}
	
	/**
	 * Sets the field(s) that are written in the destination record(s).
	 * 
	 * @param writtenFields the position(s) in the destination record(s)
	 */
	public void setWrittenFields(FieldSet writtenFields) {
		this.writtenFields = writtenFields;
	}
	
	/**
	 * Gets the field(s) in the destination record(s) that are written.
	 * 
	 * @return the field(s) in the record, or null if they are not set
	 */
	public FieldSet getWrittenFields() {
		return this.writtenFields;
	}
	
	/**
	 * Clears the object.
	 */
	public void clearProperties() {
		this.init();
	}
	
	private void init() {
		this.writtenFields = null;
	}
}
