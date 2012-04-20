/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.testing;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;

/**
 * @author arv
 */
public class PactRecordEqualer implements Equaler<PactRecord> {
	private Class<? extends Value>[] schema;

	/**
	 * Initializes PactRecordEqualer.
	 */
	public PactRecordEqualer(Class<? extends Value>[] schema) {
		this.schema = schema;
	}

	public PactRecordEqualer(Class<? extends Value> firstFieldType, Class<?>... otherFieldTypes) {
		this.schema = SchemaUtils.combineSchema(firstFieldType, otherFieldTypes);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.Equaler#equal(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean equal(PactRecord object1, PactRecord object2) {
		return recordsEqual(object1, object2, this.schema);
	}

	public static boolean recordsEqual(PactRecord object1, PactRecord object2, Class<? extends Value>[] schema) {
		if (object1.getNumFields() != schema.length)
			return false;

		for (int index = 0; index < schema.length; index++)
			if (!Equaler.SafeEquals.equal(object1.getField(index, schema[index]),
				object2.getField(index, schema[index])))
				return false;
		return true;
	}
}
