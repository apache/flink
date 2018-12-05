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
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Point-in-time configuration of a {@link GenericArraySerializer}.
 *
 * @param <C> The component type.
 */
@Internal
public final class GenericArraySerializerConfigSnapshot<C> extends CompositeTypeSerializerSnapshot<C[], GenericArraySerializer> {

	private static final int CURRENT_VERSION = 2;

	/** The class of the components of the serializer's array type. */
	@Nullable
	private Class<C> componentClass;

	/**
	 * Constructor for read instantiation.
	 */
	@SuppressWarnings("unused")
	public GenericArraySerializerConfigSnapshot() {
		super(GenericArraySerializer.class);
	}

	/**
	 * Constructor to create the snapshot for writing.
	 */
	public GenericArraySerializerConfigSnapshot(GenericArraySerializer<C> serializer) {
		super(serializer);
		this.componentClass = serializer.getComponentClass();
	}

	// ------------------------------------------------------------------------

	@Override
	public int getCurrentVersion() {
		return CURRENT_VERSION;
	}

	@Override
	protected void writeOuterSnapshot(DataOutputView out) throws IOException {
		checkState(componentClass != null);
		out.writeUTF(componentClass.getName());
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
		compositeSerializerSnapshot = CompositeSerializerSnapshot.legacyReadProductSnapshots(in, classLoader);

		try (DataInputViewStream inViewWrapper = new DataInputViewStream(in)) {
			componentClass = InstantiationUtil.deserializeObject(inViewWrapper, classLoader);
		}
		catch (ClassNotFoundException e) {
			throw new IOException("Could not find requested element class in classpath.", e);
		}
	}

	private void readV2(DataInputView in, ClassLoader classLoader) throws IOException {
		componentClass = InstantiationUtil.resolveClassByName(in, classLoader);
		compositeSerializerSnapshot = CompositeSerializerSnapshot.readCompositeSnapshot(in, classLoader);
	}

	@Override
	protected GenericArraySerializer createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
		@SuppressWarnings("unchecked")
		TypeSerializer<C> componentSerializer = (TypeSerializer<C>) nestedSerializers[0];
		return new GenericArraySerializer<>(componentClass, componentSerializer);
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializers(GenericArraySerializer outerSerializer) {
		return new TypeSerializer<?>[] { outerSerializer.getComponentSerializer() };
	}
}
