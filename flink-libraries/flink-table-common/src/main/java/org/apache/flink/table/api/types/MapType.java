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

/**
 * Map type.
 */
public class MapType extends InternalType {

	private final DataType keyType;
	private final DataType valueType;

	public MapType(DataType keyType, DataType valueType) {
		if (keyType == null) {
			throw new IllegalArgumentException("keyType should not be null.");
		}
		if (valueType == null) {
			throw new IllegalArgumentException("valueType should not be null.");
		}
		this.keyType = keyType;
		this.valueType = valueType;
	}

	public DataType getKeyType() {
		return keyType;
	}

	public DataType getValueType() {
		return valueType;
	}

	public InternalType getKeyInternalType() {
		return keyType.toInternalType();
	}

	public InternalType getValueInternalType() {
		return valueType.toInternalType();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		MapType mapType = (MapType) o;

		return getKeyInternalType().equals(mapType.getKeyInternalType()) &&
				getValueInternalType().equals(mapType.getValueInternalType());
	}

	@Override
	public int hashCode() {
		int result = getKeyInternalType().hashCode();
		result = 31 * result + getValueInternalType().hashCode();
		return result;
	}
}
