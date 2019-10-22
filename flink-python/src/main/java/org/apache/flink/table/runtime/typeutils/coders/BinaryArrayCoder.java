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
import org.apache.flink.table.dataformat.BinaryWriter;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.runtime.types.InternalSerializers;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.VarInt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * A {@link Coder} for {@link BinaryArray}.
 */
public class BinaryArrayCoder extends Coder<BinaryArray> {
	private static final long serialVersionUID = 1L;

	private final LogicalType elementType;

	private final int elementSize;

	private final Coder<Object> elementCoder;

	private final TypeSerializer eleSer;

	public static BinaryArrayCoder of(DataType elementType, Coder<?> elementCoder) {
		return new BinaryArrayCoder(elementType, elementCoder);
	}

	@SuppressWarnings("unchecked")
	private BinaryArrayCoder(DataType elementType, Coder<?> elementCoder) {
		this.elementType = LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(elementType);
		this.elementSize = BinaryArray.calculateFixLengthPartSize(this.elementType);
		this.eleSer = InternalSerializers.create(this.elementType, new ExecutionConfig());
		this.elementCoder = (Coder<Object>) elementCoder;
	}

	@Override
	public void encode(BinaryArray array, OutputStream outStream) throws IOException {
		if (array == null) {
			throw new CoderException("Cannot encode a null BinaryArray for BinaryArrayCoder");
		}
		int size = array.numElements();
		VarInt.encode(size, outStream);
		for (int i = 0; i < size; i++) {
			if (array.isNullAt(i)) {
				throw new CoderException(String.format("Cannot encode a null at position %s in BinaryArray", i));
			}
			elementCoder.encode(TypeGetterSetters.get(array, i, elementType), outStream);
		}
	}

	@Override
	public BinaryArray decode(InputStream inStream) throws IOException {
		int size = VarInt.decodeInt(inStream);
		BinaryArray array = new BinaryArray();
		BinaryArrayWriter arrayWriter = new BinaryArrayWriter(array, size, elementSize);
		for (int i = 0; i < size; i++) {
			BinaryWriter.write(arrayWriter, i, elementCoder.decode(inStream), elementType, eleSer);
		}
		arrayWriter.complete();
		return array;
	}

	@Override
	public List<? extends Coder<?>> getCoderArguments() {
		return null;
	}

	@Override
	public void verifyDeterministic() {
	}
}
