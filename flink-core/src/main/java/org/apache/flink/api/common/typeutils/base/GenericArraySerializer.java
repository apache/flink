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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializer for arrays of objects.
 * 
 * @param <C> The component type.
 */
@Internal
public final class GenericArraySerializer<C> extends TypeSerializer<C[]> {

	private static final long serialVersionUID = 1L;

	private final Class<C> componentClass;
	
	private final TypeSerializer<C> componentSerializer;
	
	private transient C[] EMPTY;
	
	
	public GenericArraySerializer(Class<C> componentClass, TypeSerializer<C> componentSerializer) {
		this.componentClass = checkNotNull(componentClass);
		this.componentSerializer = checkNotNull(componentSerializer);
	}

	public Class<C> getComponentClass() {
		return componentClass;
	}

	public TypeSerializer<C> getComponentSerializer() {
		return componentSerializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public GenericArraySerializer<C> duplicate() {
		TypeSerializer<C> duplicateComponentSerializer = this.componentSerializer.duplicate();
		if (duplicateComponentSerializer == this.componentSerializer) {
			// is not stateful, return ourselves
			return this;
		} else {
			return new GenericArraySerializer<C>(componentClass, duplicateComponentSerializer);
		}
	}

	
	@Override
	public C[] createInstance() {
		if (EMPTY == null) {
			EMPTY = create(0);
		}

		return EMPTY;
	}

	@Override
	public C[] copy(C[] from) {

		final TypeSerializer<C> serializer = this.componentSerializer;

		if (serializer.isImmutableType()) {
			return Arrays.copyOf(from, from.length);
		} else {
			C[] copy = create(from.length);
			for (int i = 0; i < copy.length; i++) {
				C val = from[i];
				if (val != null) {
					copy[i] = serializer.copy(val);
				}
			}
			return copy;
		}
	}
	
	@Override
	public C[] copy(C[] from, C[] reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(C[] value, DataOutputView target) throws IOException {
		target.writeInt(value.length);
		for (int i = 0; i < value.length; i++) {
			C val = value[i];
			if (val == null) {
				target.writeBoolean(false);
			} else {
				target.writeBoolean(true);
				componentSerializer.serialize(val, target);
			}
		}
	}

	@Override
	public C[] deserialize(DataInputView source) throws IOException {
		int len = source.readInt();
		
		C[] array = create(len);
		
		for (int i = 0; i < len; i++) {
			boolean isNonNull = source.readBoolean();
			if (isNonNull) {
				array[i] = componentSerializer.deserialize(source);
			} else {
				array[i] = null;
			}
		}
		
		return array;
	}
	
	@Override
	public C[] deserialize(C[] reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int len = source.readInt();
		target.writeInt(len);
		
		for (int i = 0; i < len; i++) {
			boolean isNonNull = source.readBoolean();
			target.writeBoolean(isNonNull);
			
			if (isNonNull) {
				componentSerializer.copy(source, target);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private final C[] create(int len) {
		return (C[]) Array.newInstance(componentClass, len);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return 31 * componentClass.hashCode() + componentSerializer.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof GenericArraySerializer) {
			GenericArraySerializer<?> other = (GenericArraySerializer<?>)obj;

			return componentClass == other.componentClass &&
				componentSerializer.equals(other.componentSerializer);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "Serializer " + componentClass.getName() + "[]";
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public GenericArraySerializerSnapshot<C> snapshotConfiguration() {
		return new GenericArraySerializerSnapshot<>(this);
	}
}
