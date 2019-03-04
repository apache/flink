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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.type.InternalType;

/**
 * TypeInfo for BaseArray.
 */
public class BinaryArrayTypeInfo extends TypeInformation<BinaryArray> {

	private static final long serialVersionUID = 1L;

	private final InternalType elementType;

	public BinaryArrayTypeInfo(InternalType elementType) {
		this.elementType = elementType;
	}

	public InternalType getElementType() {
		return elementType;
	}

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
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<BinaryArray> getTypeClass() {
		return BinaryArray.class;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<BinaryArray> createSerializer(ExecutionConfig executionConfig) {
		return BinaryArraySerializer.INSTANCE;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "<" + this.elementType + ">";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BinaryArrayTypeInfo) {
			BinaryArrayTypeInfo typeInfo = (BinaryArrayTypeInfo) obj;

			return typeInfo.canEqual(this) &&
					elementType.equals(typeInfo.elementType);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BinaryArrayTypeInfo;
	}

	@Override
	public int hashCode() {
		return this.elementType.hashCode();
	}
}
