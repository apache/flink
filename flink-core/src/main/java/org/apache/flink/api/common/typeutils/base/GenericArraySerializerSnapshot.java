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

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * Point-in-time configuration of a {@link GenericArraySerializer}.
 *
 * @param <C> The component type.
 */
public final class GenericArraySerializerSnapshot<C> extends CompositeTypeSerializerSnapshot<C[], GenericArraySerializer<C>> {

	private static final int CURRENT_VERSION = 1;

	private Class<C> componentClass;

	/**
	 * Constructor to be used for read instantiation.
	 */
	public GenericArraySerializerSnapshot() {
		super(GenericArraySerializer.class);
	}

	/**
	 * Constructor to be used for writing the snapshot.
	 */
	public GenericArraySerializerSnapshot(GenericArraySerializer<C> genericArraySerializer) {
		super(genericArraySerializer);
		this.componentClass = genericArraySerializer.getComponentClass();
	}

	/**
	 * Constructor that the legacy {@link GenericArraySerializerConfigSnapshot} uses
	 * to delegate compatibility checks to this class.
	 */
	@SuppressWarnings("deprecation")
	GenericArraySerializerSnapshot(Class<C> componentClass) {
		super(GenericArraySerializer.class);
		this.componentClass = componentClass;
	}

	@Override
	protected int getCurrentOuterSnapshotVersion() {
		return CURRENT_VERSION;
	}

	@Override
	protected void writeOuterSnapshot(DataOutputView out) throws IOException {
		out.writeUTF(componentClass.getName());
	}

	@Override
	protected void readOuterSnapshot(int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		this.componentClass = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);
	}

	@Override
	protected boolean isOuterSnapshotCompatible(GenericArraySerializer<C> newSerializer) {
		return this.componentClass == newSerializer.getComponentClass();
	}

	@Override
	protected GenericArraySerializer<C> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
		@SuppressWarnings("unchecked")
		TypeSerializer<C> componentSerializer = (TypeSerializer<C>) nestedSerializers[0];
		return new GenericArraySerializer<>(componentClass, componentSerializer);
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializers(GenericArraySerializer<C> outerSerializer) {
		return new TypeSerializer<?>[] { outerSerializer.getComponentSerializer() };
	}
}
