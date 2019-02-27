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

package org.apache.flink.table.type;

import org.apache.flink.util.Preconditions;

/**
 * Type for Array.
 */
public class ArrayType implements InternalType {

	private final InternalType elementType;
	private final boolean primitive;

	public ArrayType(InternalType elementType) {
		this(elementType, false);
	}

	public ArrayType(InternalType elementType, boolean primitive) {
		this.elementType = Preconditions.checkNotNull(elementType);
		if (primitive) {
			Preconditions.checkArgument(elementType instanceof PrimitiveType);
		}
		this.primitive = primitive;
	}

	public InternalType getElementType() {
		return elementType;
	}

	public boolean isPrimitive() {
		return primitive;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ArrayType arrayType = (ArrayType) o;

		return primitive == arrayType.primitive && elementType.equals(arrayType.elementType);
	}

	@Override
	public int hashCode() {
		int result = elementType != null ? elementType.hashCode() : 0;
		result = 31 * result + (primitive ? 1 : 0);
		return result;
	}

	@Override
	public String toString() {
		return "ArrayType{" +
				"elementType=" + elementType +
				", primitive=" + primitive +
				'}';
	}
}
