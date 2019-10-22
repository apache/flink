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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.typeutils.coders.ArrayCoder;
import org.apache.flink.table.runtime.typeutils.coders.BinaryArrayCoder;
import org.apache.flink.table.runtime.typeutils.coders.BinaryMapCoder;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypeVisitor;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MultisetType;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.VarIntCoder;

/**
 * The coder factory for {@link CollectionDataType}.
 */
public class CollectionDataTypeCoderFactory implements DataTypeCoderFactory<CollectionDataType> {
	public static CollectionDataTypeCoderFactory of(DataTypeVisitor<Coder> dataTypeVisitor, boolean isBlinkPlanner) {
		return new CollectionDataTypeCoderFactory(dataTypeVisitor, isBlinkPlanner);
	}

	private final DataTypeVisitor<Coder> dataTypeVisitor;

	private final boolean isBlinkPlanner;

	private CollectionDataTypeCoderFactory(DataTypeVisitor<Coder> dataTypeVisitor, boolean isBlinkPlanner) {
		this.dataTypeVisitor = dataTypeVisitor;
		this.isBlinkPlanner = isBlinkPlanner;
	}

	@Override
	public Coder findCoder(CollectionDataType dataType) {
		DataType elementType = dataType.getElementDataType();
		LogicalType logicalType = dataType.getLogicalType();
		Coder<?> elementCoder = elementType.accept(dataTypeVisitor);
		if (logicalType instanceof ArrayType) {
			if (isBlinkPlanner) {
				return BinaryArrayCoder.of(elementType, elementCoder);
			}
			return ArrayCoder.of(elementCoder, elementType.getConversionClass());
		} else if (logicalType instanceof MultisetType) {
			if (isBlinkPlanner) {
				return BinaryMapCoder.of(elementType, DataTypes.INT(), elementCoder, VarIntCoder.of());
			}
			return MapCoder.of(elementCoder, VarIntCoder.of());
		}
		throw new IllegalArgumentException("No matched CollectionDataType Coder for " + logicalType);
	}
}
