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
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.dataformat.BaseArray;

/**
 * TypeInfo for BaseArray.
 */
public class BaseArrayTypeInfo extends TypeInformation<BaseArray> {

	private static final long serialVersionUID = 1L;

	private final boolean isPrimitive;
	private final DataType eleType;

	public BaseArrayTypeInfo(boolean isPrimitive, DataType eleType) {
		this.isPrimitive = isPrimitive;
		this.eleType = eleType;
	}

	// --------------------------------------------------------------------------------------------

	public boolean isPrimitive() {
		return isPrimitive;
	}

	public DataType getEleType() {
		return eleType;
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
	public Class<BaseArray> getTypeClass() {
		return BaseArray.class;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<BaseArray> createSerializer(ExecutionConfig executionConfig) {
		return new BaseArraySerializer(isPrimitive, eleType.toInternalType());
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "<" + this.eleType.toInternalType() + ">";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BaseArrayTypeInfo) {
			BaseArrayTypeInfo typeInfo = (BaseArrayTypeInfo) obj;

			return typeInfo.canEqual(this) &&
				isPrimitive == typeInfo.isPrimitive &&
				eleType.toInternalType().equals(typeInfo.eleType.toInternalType());
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BaseArrayTypeInfo;
	}

	@Override
	public int hashCode() {
		return 31 * Boolean.hashCode(isPrimitive) + this.eleType.toInternalType().hashCode();
	}
}
