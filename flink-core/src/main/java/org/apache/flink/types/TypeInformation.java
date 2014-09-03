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

package org.apache.flink.types;

import org.apache.flink.api.common.typeutils.TypeSerializer;

public abstract class TypeInformation<T> {
	protected final String NO_POJO_WARNING = "Key expressions can only be used on POJOs." + " " +
			"A POJO must have a default constructor without arguments and not have readObject" +
			" and/or writeObject methods. A current restriction is that it can only have nested POJOs or primitive" +
			"(also boxed) fields.";
	
	public abstract boolean isBasicType();
	
	public abstract boolean isTupleType();
	
	public abstract int getArity();
	
	public abstract Class<T> getTypeClass();
	
	public abstract boolean isKeyType();
	
	public abstract TypeSerializer<T> createSerializer();

	public int[] getLogicalPositions(String[] fieldExpression) {
		throw new UnsupportedOperationException(NO_POJO_WARNING);
	}

	public TypeInformation<?>[] getTypes(String[] fieldExpression) {
		throw new UnsupportedOperationException(NO_POJO_WARNING);
	}
}
