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

package org.apache.flink.table.data.binary;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.util.WrappingRuntimeException;

import java.io.IOException;

/**
 * An abstract implementation fo {@link BinaryFormat} which is lazily serialized into binary
 * or lazily deserialized into Java object.
 *
 * <p>The reason why we introduce this data structure is in order to save (de)serialization
 * in nested function calls. Consider the following function call chain:
 *
 * <pre>UDF0(input) -> UDF1(result0) -> UDF2(result1) -> UDF3(result2)</pre>
 *
 * <p>Such nested calls, if the return values of UDFs are Java object format,
 * it will result in multiple conversions between Java object and binary format:
 *
 * <pre>
 * converterToBinary(UDF0(converterToJavaObject(input))) ->
 *   converterToBinary(UDF1(converterToJavaObject(result0))) ->
 *     converterToBinary(UDF2(converterToJavaObject(result1))) ->
 *       ...
 * </pre>
 *
 * <p>So we introduced {@link LazyBinaryFormat} to avoid the redundant cost, it has three forms:
 * <ul>
 *     <li>Binary form</li>
 *     <li>Java object form</li>
 *     <li>Binary and Java object both exist</li>
 * </ul>
 *
 * <p>It can lazy the conversions as much as possible. It will be converted into required form
 * only when it is needed.
 */
@Internal
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

	public BinarySection getBinarySection() {
		return binarySection;
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
	 * (For example, {@link RawValueData} needs javaObjectSerializer).
	 */
	protected abstract BinarySection materialize(TypeSerializer<T> serializer) throws IOException;
}
