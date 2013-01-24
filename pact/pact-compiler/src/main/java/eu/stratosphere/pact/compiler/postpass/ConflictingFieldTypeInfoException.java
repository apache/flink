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

import eu.stratosphere.pact.common.type.Value;

@SuppressWarnings("serial")
public final class ConflictingFieldTypeInfoException extends Exception
{
	private final int fieldNumber;
	
	private final Class<? extends Value> previousType, newType;

	
	public ConflictingFieldTypeInfoException(int fieldNumber, Class<? extends Value> previousType, Class<? extends Value> newType) {
		this.fieldNumber = fieldNumber;
		this.previousType = previousType;
		this.newType = newType;
	}
	
	
	public int getFieldNumber() {
		return fieldNumber;
	}

	public Class<? extends Value> getPreviousType() {
		return this.previousType;
	}

	public Class<? extends Value> getNewType() {
		return this.newType;
	}
}