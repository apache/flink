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
import org.apache.flink.table.dataformat.BaseMap;

/**
 * TypeInfo for BaseMap.
 */
public class BaseMapTypeInfo extends TypeInformation<BaseMap> {

	private static final long serialVersionUID = 1L;

	private final DataType keyType;
	private final DataType valueType;

	public BaseMapTypeInfo(DataType keyType, DataType valueType) {
		this.keyType = keyType;
		this.valueType = valueType;
	}

	// --------------------------------------------------------------------------------------------

	public DataType getKeyType() {
		return keyType;
	}

	public DataType getValueType() {
		return valueType;
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
	public Class<BaseMap> getTypeClass() {
		return BaseMap.class;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<BaseMap> createSerializer(ExecutionConfig executionConfig) {
		return new BaseMapSerializer(keyType.toInternalType(), valueType.toInternalType());
	}

	@Override
	public String toString() {
		return  this.getClass().getSimpleName() +
				"<" + keyType.toInternalType() + ", " + valueType.toInternalType() + ">";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BaseMapTypeInfo) {
			BaseMapTypeInfo typeInfo = (BaseMapTypeInfo) obj;

			return typeInfo.canEqual(this) &&
					keyType.toInternalType() == typeInfo.keyType.toInternalType() &&
					valueType.toInternalType().equals(typeInfo.valueType.toInternalType());
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BaseMapTypeInfo;
	}

	@Override
	public int hashCode() {
		return 31 * keyType.toInternalType().hashCode() + valueType.toInternalType().hashCode();
	}
}
