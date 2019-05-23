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

	private static final long serialVersionUID = 1L;

	private final InternalType elementType;

	public ArrayType(InternalType elementType) {
		this.elementType = Preconditions.checkNotNull(elementType);
	}

	public InternalType getElementType() {
		return elementType;
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

		return elementType.equals(arrayType.elementType);
	}

	@Override
	public int hashCode() {
		return elementType.hashCode();
	}

	@Override
	public String toString() {
		return "ArrayType{elementType=" + elementType + '}';
	}
}
