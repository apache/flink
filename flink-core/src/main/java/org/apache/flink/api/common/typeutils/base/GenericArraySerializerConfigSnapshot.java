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
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Point-in-time configuration of a {@link GenericArraySerializer}.
 *
 * @param <C> The component type.
 */
@Internal
public final class GenericArraySerializerConfigSnapshot<C> extends CompositeTypeSerializerConfigSnapshot {

	private static final int VERSION = 1;

	private Class<C> componentClass;

	/** This empty nullary constructor is required for deserializing the configuration. */
	public GenericArraySerializerConfigSnapshot() {}

	public GenericArraySerializerConfigSnapshot(
			Class<C> componentClass,
			TypeSerializer<C> componentSerializer) {

		super(componentSerializer);

		this.componentClass = Preconditions.checkNotNull(componentClass);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		try (final DataOutputViewStream outViewWrapper = new DataOutputViewStream(out)) {
			InstantiationUtil.serializeObject(outViewWrapper, componentClass);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		try (final DataInputViewStream inViewWrapper = new DataInputViewStream(in)) {
			componentClass = InstantiationUtil.deserializeObject(inViewWrapper, getUserCodeClassLoader());
		} catch (ClassNotFoundException e) {
			throw new IOException("Could not find requested element class in classpath.", e);
		}
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	public Class<C> getComponentClass() {
		return componentClass;
	}

	@Override
	public boolean equals(Object obj) {
		return super.equals(obj)
			&& (obj instanceof GenericArraySerializerConfigSnapshot)
			&& (componentClass.equals(((GenericArraySerializerConfigSnapshot) obj).getComponentClass()));
	}

	@Override
	public int hashCode() {
		return super.hashCode() * 31 + componentClass.hashCode();
	}
}
