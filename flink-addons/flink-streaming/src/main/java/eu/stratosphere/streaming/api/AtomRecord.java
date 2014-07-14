/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.api;

import eu.stratosphere.types.Value;

/**
 * Basic record object that holds a fixed number of Values (fields)
 * 
 * 
 */
public class AtomRecord {
	private Value[] record;

	/**
	 * Creates an AtomRecord with one field
	 * 
	 */
	public AtomRecord() {
		record = new Value[1];
	}

	/**
	 * Creates an AtomRecord with the given number of fields
	 * 
	 * @param length
	 *          Number of fields
	 */
	public AtomRecord(int length) {
		record = new Value[length];
	}

	/**
	 * Creates an AtomRecord from a Value array that will hold the same number of
	 * fields and values
	 * 
	 * @param fields
	 *          Value array of the desired fields
	 */
	public AtomRecord(Value[] fields) {
		record = fields;
	}

	/**
	 * Creates an AtomRecord that hold one field with the given Value
	 * 
	 * @param field
	 *          Desired value
	 */
	public AtomRecord(Value field) {
		record = new Value[1];
		record[0] = field;
	}

	/**
	 * 
	 * @return Value array containing the fields
	 */
	public Value[] getFields() {
		return record;
	}

	/**
	 * 
	 * @param fieldNumber
	 *          The position of the desired field
	 * @return Value in that position
	 */
	public Value getField(int fieldNumber) {
		return record[fieldNumber];
	}

	/**
	 * 
	 * @param fieldNumber
	 *          The position of the field that will be set
	 * @param value
	 *          New value of the field
	 */
	public void setField(int fieldNumber, Value value) {
		record[fieldNumber] = value;
	}

}
