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

package org.apache.flink.table.api.types;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Type for Array.
 */
public class ArrayType extends InternalType {

	private boolean isPrimitive;
	private DataType elementType;

	public ArrayType(DataType elementType) {
		this(elementType, false);
	}

	public ArrayType(DataType elementType, boolean isPrimitive) {
		this.elementType = checkNotNull(elementType);
		this.isPrimitive = isPrimitive;
	}

	public boolean isPrimitive() {
		return isPrimitive;
	}

	public DataType getElementType() {
		return elementType;
	}

	public InternalType getElementInternalType() {
		return elementType.toInternalType();
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

		return isPrimitive == arrayType.isPrimitive &&
				getElementInternalType().equals(arrayType.getElementInternalType());
	}

	@Override
	public int hashCode() {
		int result = (isPrimitive ? 1 : 0);
		result = 31 * result + getElementInternalType().hashCode();
		return result;
	}
}
