/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.pact.common.type.Value;

/**
 * @author Arvid Heise
 */
public class SchemaUtils {

	@SuppressWarnings("unchecked")
	public static Class<? extends Value>[] combineSchema(Class<? extends Value> firstFieldType,
			Class<?>... otherFieldTypes) {
		Class<? extends Value>[] schema = new Class[1 + otherFieldTypes.length];
		schema[0] = firstFieldType;
		for (int index = 0; index < otherFieldTypes.length; index++) {
			if (!Value.class.isAssignableFrom(otherFieldTypes[index]))
				throw new IllegalArgumentException("All schema types must implement Value");
			schema[index + 1] = (Class<? extends Value>) otherFieldTypes[index];
		}

		return schema;
	}
}
