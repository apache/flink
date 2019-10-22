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

package org.apache.flink.table.runtime.typeutils.coders;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryArrayWriter;
import org.apache.flink.table.dataformat.BinaryMap;
import org.apache.flink.table.dataformat.BinaryWriter;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.runtime.types.InternalSerializers;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * A {@link Coder} for {@link BinaryMap}.
 */
public class BinaryMapCoder extends Coder<BinaryMap> {

	private static final long serialVersionUID = 1L;

	private final LogicalType keyType;

	private final LogicalType valueType;

	private final int keyElementSize;

	private final int valueElementSize;

	private final TypeSerializer keySer;

	private final TypeSerializer valueSer;

	private final Coder<Object> keyCoder;

	private final Coder<Object> valueCoder;

	public static BinaryMapCoder of(DataType keyDataType, DataType valueDataType, Coder<?> keyCoder, Coder<?> valueCoder) {
		return new BinaryMapCoder(keyDataType, valueDataType, keyCoder, valueCoder);
	}

	@SuppressWarnings("unchecked")
	private BinaryMapCoder(DataType keyDataType, DataType valueDataType, Coder<?> keyCoder, Coder<?> valueCoder) {
		this.keyType = LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(keyDataType);
		this.valueType = LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(valueDataType);
		this.keyElementSize = BinaryArray.calculateFixLengthPartSize(keyType);
		this.valueElementSize = BinaryArray.calculateFixLengthPartSize(valueType);
		this.keySer = InternalSerializers.create(this.keyType, new ExecutionConfig());
		this.valueSer = InternalSerializers.create(this.valueType, new ExecutionConfig());
		this.keyCoder = (Coder<Object>) keyCoder;
		this.valueCoder = (Coder<Object>) valueCoder;
	}

	@Override
	public void encode(BinaryMap map, OutputStream outStream) throws IOException {
		if (map == null) {
			throw new CoderException("Cannot encode a null BinaryMap for BinaryMapCoder");
		}
		DataOutputStream dataOutStream = new DataOutputStream(outStream);
		int size = map.numElements();
		dataOutStream.writeInt(size);
		BinaryArray keyArray = map.keyArray();
		BinaryArray valueArray = map.valueArray();
		for (int i = 0; i < size; i++) {
			if (keyArray.isNullAt(i)) {
				throw new CoderException(String.format("Cannot encode a null key at position %s in BinaryMap", i));
			}
			if (valueArray.isNullAt(i)) {
				throw new CoderException(String.format("Cannot encode a null value at position %s in BinaryMap", i));
			}
			keyCoder.encode(TypeGetterSetters.get(keyArray, i, keyType), outStream);
			valueCoder.encode(TypeGetterSetters.get(valueArray, i, valueType), outStream);
		}
	}

	@Override
	public BinaryMap decode(InputStream inStream) throws IOException {
		DataInputStream dataInStream = new DataInputStream(inStream);
		int size = dataInStream.readInt();
		BinaryArray keyArray = new BinaryArray();
		BinaryArray valueArray = new BinaryArray();
		BinaryArrayWriter keyWriter = new BinaryArrayWriter(keyArray, size, keyElementSize);
		BinaryArrayWriter valueWriter = new BinaryArrayWriter(valueArray, size, valueElementSize);
		for (int i = 0; i < size; i++) {
			BinaryWriter.write(keyWriter, i, keyCoder.decode(inStream), keyType, keySer);
			BinaryWriter.write(valueWriter, i, valueCoder.decode(inStream), valueType, valueSer);
		}
		keyWriter.complete();
		valueWriter.complete();
		return BinaryMap.valueOf(keyArray, valueArray);
	}

	@Override
	public List<? extends Coder<?>> getCoderArguments() {
		return null;
	}

	@Override
	public void verifyDeterministic() {
	}
}
