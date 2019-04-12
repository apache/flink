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

/**
 * Map type.
 */
public class MapType implements InternalType {

	private static final long serialVersionUID = 1L;

	private final InternalType keyType;
	private final InternalType valueType;

	public MapType(InternalType keyType, InternalType valueType) {
		if (keyType == null) {
			throw new IllegalArgumentException("keyType should not be null.");
		}
		if (valueType == null) {
			throw new IllegalArgumentException("valueType should not be null.");
		}
		this.keyType = keyType;
		this.valueType = valueType;
	}

	public InternalType getKeyType() {
		return keyType;
	}

	public InternalType getValueType() {
		return valueType;
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

		return getKeyType().equals(mapType.getKeyType()) &&
				getValueType().equals(mapType.getValueType());
	}

	@Override
	public int hashCode() {
		int result = getKeyType().hashCode();
		result = 31 * result + getValueType().hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "MapType{" +
				"keyType=" + keyType +
				", valueType=" + valueType +
				'}';
	}
}
