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

package org.apache.flink.table.runtime.typeutils.coders.factory;

import org.apache.flink.table.runtime.typeutils.coders.BinaryMapCoder;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypeVisitor;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.MapCoder;

/**
 * The coder factory for {@link KeyValueDataType}.
 */
public class KeyValueDataTypeCoderFactory implements DataTypeCoderFactory<KeyValueDataType> {
	public static KeyValueDataTypeCoderFactory of(DataTypeVisitor<Coder> dataTypeVisitor, boolean isBlinkPlanner) {
		return new KeyValueDataTypeCoderFactory(dataTypeVisitor, isBlinkPlanner);
	}

	private final DataTypeVisitor<Coder> dataTypeVisitor;

	private final boolean isBlinkPlanner;

	private KeyValueDataTypeCoderFactory(DataTypeVisitor<Coder> dataTypeVisitor, boolean isBlinkPlanner) {
		this.dataTypeVisitor = dataTypeVisitor;
		this.isBlinkPlanner = isBlinkPlanner;
	}

	@Override
	public Coder findCoder(KeyValueDataType dataType) {
		LogicalType logicalType = dataType.getLogicalType();
		DataType keyDataType = dataType.getKeyDataType();
		DataType valueDataType = dataType.getValueDataType();
		Coder<?> keyCoder = keyDataType.accept(dataTypeVisitor);
		Coder<?> valueCoder = valueDataType.accept(dataTypeVisitor);
		if (logicalType instanceof MapType) {
			if (isBlinkPlanner) {
				return BinaryMapCoder.of(keyDataType, valueDataType, keyCoder, valueCoder);
			}
			return MapCoder.of(keyCoder, valueCoder);
		}
		throw new IllegalArgumentException("No matched KeyValueDataType Coder for " + logicalType);
	}
}
