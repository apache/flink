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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Point-in-time configuration of a {@link GenericArraySerializer}.
 *
 * @param <C> The component type.
 */
@Internal
public final class GenericArraySerializerConfigSnapshot<C>
		extends CompositeSerializerSnapshot<C[], GenericArraySerializer<C>> {

	private static final int CURRENT_VERSION = 2;

	private Class<C> componentClass;

	/** Ctor for read instantiation. */
	@SuppressWarnings("unused")
	public GenericArraySerializerConfigSnapshot() {}

	/** Ctor for writing snapshot. */
	public GenericArraySerializerConfigSnapshot(GenericArraySerializer<C> serializer) {
		super(serializer.getComponentSerializer());
		this.componentClass = serializer.getComponentClass();
	}

	// ------------------------------------------------------------------------
	//  Serialization
	// ------------------------------------------------------------------------

	@Override
	public int getCurrentVersion() {
		return CURRENT_VERSION;
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		out.writeUTF(componentClass.getName());
		writeProductSnapshots(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader) throws IOException {
		switch (readVersion) {
			case 1:
				readV1(in, classLoader);
				break;
			case 2:
				readV2(in, classLoader);
				break;
			default:
				throw new IllegalArgumentException("Unrecognized version: " + readVersion);
		}
	}

	private void readV1(DataInputView in, ClassLoader classLoader) throws IOException {
		legacyReadProductSnapshots(in, classLoader);

		try (DataInputViewStream inViewWrapper = new DataInputViewStream(in)) {
			componentClass = InstantiationUtil.deserializeObject(inViewWrapper, classLoader);
		}
		catch (ClassNotFoundException e) {
			throw new IOException("Could not find requested element class in classpath.", e);
		}
	}

	private void readV2(DataInputView in, ClassLoader classLoader) throws IOException {
		this.componentClass = InstantiationUtil.resolveClassByName(in, classLoader);
		readProductSnapshots(in, classLoader);
	}

	// ------------------------------------------------------------------------
	//  Product Serializer Snapshot Methods
	// ------------------------------------------------------------------------

	@Override
	protected Class<?> outerSerializerType() {
		return GenericArraySerializer.class;
	}

	@Override
	protected TypeSerializer<C[]> createSerializer(TypeSerializer<?>... nestedSerializers) {
		checkArgument(nestedSerializers.length == 1);

		@SuppressWarnings("unchecked")
		TypeSerializer<C> componentSerializer = (TypeSerializer<C>) nestedSerializers[0];

		return new GenericArraySerializer<>(componentClass, componentSerializer);
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializersFromSerializer(GenericArraySerializer<C> serializer) {
		return new TypeSerializer<?>[] { serializer.getComponentSerializer() };
	}

	@Override
	protected TypeSerializerSchemaCompatibility<C[]>
	outerCompatibility(GenericArraySerializer<C> serializer) {

		return serializer.getComponentClass() == componentClass ?
				TypeSerializerSchemaCompatibility.compatibleAsIs() :
				TypeSerializerSchemaCompatibility.incompatible();
	}
}
