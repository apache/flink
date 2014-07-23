/**
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

package org.apache.flink.api.java.typeutils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

// We use these in PojoTypeInfo and PojoComparator for accessing the fields. Having a chain of
// field accessors allows access to fields of nested objects.
class PojoFieldAccessor {
	// we keep these in a list in case we need to access fields of nested objects
	// for "foo.bar" we would have to Field instances in the list on for getting foo
	// and then one for getting bar of the return value of the first field accessor
	public List<Field> accessorChain;
	public PojoField pojoField;

	public PojoFieldAccessor(List<Field> accessorChain, Field field, PojoField pojoField) {
		this.accessorChain = new ArrayList<Field>();
		this.accessorChain.addAll(accessorChain);
		this.accessorChain.add(field);
		this.pojoField = pojoField;
	}

	@Override
	public String toString() {
		Joiner join = Joiner.on('.');
		List<String> fieldNames = new ArrayList<String>();
		for (Field f : accessorChain) {
			fieldNames.add(f.getName());
		}
		String fields = join.join(fieldNames);
		return "Field Accessor" + accessorChain.get(0).getDeclaringClass() + "." + fields + " (" + pojoField.type + ")";
	}
}