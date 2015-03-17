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

public final class ConflictingFieldTypeInfoException extends Exception {

	private static final long serialVersionUID = 3991352502693288321L;

	private final int fieldNumber;
	
	private final Object previousType, newType;

	
	public ConflictingFieldTypeInfoException(int fieldNumber, Object previousType, Object newType) {
		super("Conflicting type info for field " + fieldNumber + ": Old='" + previousType + "', new='" + newType + "'.");
		this.fieldNumber = fieldNumber;
		this.previousType = previousType;
		this.newType = newType;
	}
	
	
	public int getFieldNumber() {
		return fieldNumber;
	}

	public Object getPreviousType() {
		return this.previousType;
	}

	public Object getNewType() {
		return this.newType;
	}
}
