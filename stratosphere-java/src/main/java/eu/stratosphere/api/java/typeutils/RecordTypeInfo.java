/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.typeutils;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordSerializer;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.TypeInformation;


/**
 * Type information for the record API.
 */
public class RecordTypeInfo extends TypeInformation<Record> {

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public Class<Record> getTypeClass() {
		return Record.class;
	}
	
	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<Record> createSerializer() {
		return RecordSerializer.get();
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return Record.class.hashCode() ^ 0x165667b1;
	}
	
	@Override
	public boolean equals(Object obj) {
		return obj.getClass() == RecordTypeInfo.class;
	}
	
	@Override
	public String toString() {
		return "RecordType";
	}
}
