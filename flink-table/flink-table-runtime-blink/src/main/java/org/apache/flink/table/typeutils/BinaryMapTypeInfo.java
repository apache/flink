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
import org.apache.flink.table.dataformat.BinaryMap;
import org.apache.flink.table.type.InternalType;

/**
 * TypeInfo for BaseMap.
 */
public class BinaryMapTypeInfo extends TypeInformation<BinaryMap> {

	private static final long serialVersionUID = 1L;

	private final InternalType keyType;
	private final InternalType valueType;

	public BinaryMapTypeInfo(InternalType keyType, InternalType valueType) {
		this.keyType = keyType;
		this.valueType = valueType;
	}

	// --------------------------------------------------------------------------------------------

	public InternalType getKeyType() {
		return keyType;
	}

	public InternalType getValueType() {
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
	public Class<BinaryMap> getTypeClass() {
		return BinaryMap.class;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<BinaryMap> createSerializer(ExecutionConfig executionConfig) {
		return BinaryMapSerializer.INSTANCE;
	}

	@Override
	public String toString() {
		return  this.getClass().getSimpleName() +
				"<" + keyType + ", " + valueType + ">";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BinaryMapTypeInfo) {
			BinaryMapTypeInfo typeInfo = (BinaryMapTypeInfo) obj;

			return keyType.equals(typeInfo.keyType) && valueType.equals(typeInfo.valueType);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BinaryMapTypeInfo;
	}

	@Override
	public int hashCode() {
		return 31 * keyType.hashCode() + valueType.hashCode();
	}
}
