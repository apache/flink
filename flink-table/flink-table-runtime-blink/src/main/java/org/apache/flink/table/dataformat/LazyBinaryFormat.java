/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.WrappingRuntimeException;

import java.io.IOException;

/**
 * Lazy binary format.
 *
 * <p>If the Binary format is used in all the SQL representations, consider the following Case:
 *   udf0(input) -> udf1(result0) -> udf2(result1) ->udf3 (result2)
 * Such nested calls, if udf's return values are JavaObject format, will result in multiple
 * conversions if store local variables in Binary format:
 *   converterToBinary(Udf0(converterToJavaObject(input))) ->
 *         converterToBinary(Udf1(converterToJavaObject(result0))) .....
 *
 * <p>So we introduced Lazy Binary Format to avoid this situation, Lazy Binary Format has three
 * forms:
 *   1. Binary form
 *   2. JavaObject form
 *   3. Binary and JavaObject coexist
 * It can lazy the conversions as much as possible. Only when it is needed can it be converted
 * into the required form.
 */
public abstract class LazyBinaryFormat<T> implements BinaryFormat {

	T javaObject;
	BinarySection binarySection;

	public LazyBinaryFormat() {
		this(null, -1, -1, null);
	}

	public LazyBinaryFormat(MemorySegment[] segments, int offset, int sizeInBytes, T javaObject) {
		this(javaObject, new BinarySection(segments, offset, sizeInBytes));
	}

	public LazyBinaryFormat(MemorySegment[] segments, int offset, int sizeInBytes) {
		this(null, new BinarySection(segments, offset, sizeInBytes));
	}

	public LazyBinaryFormat(T javaObject) {
		this(javaObject, null);
	}

	public LazyBinaryFormat(T javaObject, BinarySection binarySection) {
		this.javaObject = javaObject;
		this.binarySection = binarySection;
	}

	public T getJavaObject() {
		return javaObject;
	}

	/**
	 * Must be public as it is used during code generation.
	 */
	public void setJavaObject(T javaObject) {
		this.javaObject = javaObject;
	}

	@Override
	public MemorySegment[] getSegments() {
		if (binarySection == null) {
			throw new IllegalStateException("Lazy Binary Format was not materialized");
		}
		return binarySection.segments;
	}

	@Override
	public int getOffset() {
		if (binarySection == null) {
			throw new IllegalStateException("Lazy Binary Format was not materialized");
		}
		return binarySection.offset;
	}

	@Override
	public int getSizeInBytes() {
		if (binarySection == null) {
			throw new IllegalStateException("Lazy Binary Format was not materialized");
		}
		return binarySection.sizeInBytes;
	}

	/**
	 * Ensure we have materialized binary format.
	 */
	public final void ensureMaterialized(TypeSerializer<T> serializer) {
		if (binarySection == null) {
			try {
				this.binarySection = materialize(serializer);
			} catch (IOException e) {
				throw new WrappingRuntimeException(e);
			}
		}
	}

	/**
	 * Materialize java object to binary format.
	 * Inherited classes need to hold the information they need.
	 * (For example, BinaryGeneric needs javaObjectSerializer).
	 */
	protected abstract BinarySection materialize(TypeSerializer<T> serializer) throws IOException;
}
