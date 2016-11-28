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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static java.util.Objects.requireNonNull;

/**
 * {@link StateDescriptor} for {@link FoldingState}. This can be used to create partitioned
 * folding state.
 *
 * @param <T> Type of the values folded int othe state
 * @param <ACC> Type of the value in the state
 */
@PublicEvolving
public class FoldingStateDescriptor<T, ACC> extends SimpleStateDescriptor<ACC, FoldingState<T, ACC>> {
	private static final long serialVersionUID = 1L;

	private transient ACC initialValue;
	private final FoldFunction<T, ACC> foldFunction;

	/**
	 * Creates a new {@code FoldingStateDescriptor} with the given name, type, and initial value.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #FoldingStateDescriptor(String, ACC, FoldFunction, TypeInformation)} constructor.
	 *
	 * @param name The (unique) name for the state.
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The {@code FoldFunction} used to aggregate the state.
	 * @param typeClass The type of the values in the state.
	 */
	public FoldingStateDescriptor(String name, ACC initialValue, FoldFunction<T, ACC> foldFunction, Class<ACC> typeClass) {
		super(name, typeClass);

		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of FoldingState can not be a RichFunction.");
		}

		this.foldFunction = requireNonNull(foldFunction);
		this.initialValue = initialValue;
	}

	/**
	 * Creates a new {@code FoldingStateDescriptor} with the given name and default value.
	 *
	 * @param name The (unique) name for the state.
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The {@code FoldFunction} used to aggregate the state.
	 * @param typeInfo The type of the values in the state.
	 */
	public FoldingStateDescriptor(String name, ACC initialValue, FoldFunction<T, ACC> foldFunction, TypeInformation<ACC> typeInfo) {
		super(name, typeInfo);

		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of FoldingState can not be a RichFunction.");
		}

		this.foldFunction = requireNonNull(foldFunction);
		this.initialValue = initialValue;
	}

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name and default value.
	 *
	 * @param name The (unique) name for the state.
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The {@code FoldFunction} used to aggregate the state.
	 * @param typeSerializer The type serializer of the values in the state.
	 */
	public FoldingStateDescriptor(String name, ACC initialValue, FoldFunction<T, ACC> foldFunction, TypeSerializer<ACC> typeSerializer) {
		super(name, typeSerializer);

		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of FoldingState can not be a RichFunction.");
		}

		this.foldFunction = requireNonNull(foldFunction);
		this.initialValue = initialValue;
	}

	// ------------------------------------------------------------------------

	/**
	 * Returns the initial value used in the folding.
	 */
	public ACC getInitialValue() {
		if (initialValue != null) {
			if (typeSerializer != null) {
				return typeSerializer.copy(initialValue);
			} else {
				throw new IllegalStateException("Serializer not yet initialized.");
			}
		} else {
			return null;
		}
	}

	/**
	 * Returns the fold function to be used for the folding state.
	 */
	public FoldFunction<T, ACC> getFoldFunction() {
		return foldFunction;
	}

	// ------------------------------------------------------------------------

	@Override
	public FoldingState<T, ACC> bind(StateBackend stateBackend) throws Exception {
		return stateBackend.createFoldingState(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		FoldingStateDescriptor<?, ?> that = (FoldingStateDescriptor<?, ?>) o;

		return typeSerializer.equals(that.typeSerializer) && name.equals(that.name);

	}

	@Override
	public int hashCode() {
		int result = typeSerializer.hashCode();
		result = 31 * result + name.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "FoldingStateDescriptor{" +
				"serializer=" + typeSerializer +
				", initialValue=" + initialValue +
				", foldFunction=" + foldFunction +
				'}';
	}

	// ------------------------------------------------------------------------
	//  Serialization
	// ------------------------------------------------------------------------

	private void writeObject(final ObjectOutputStream out) throws IOException {
		// write the fold function
		out.defaultWriteObject();

		// write the non-serializable default value field
		if (initialValue == null) {
			// we don't have an initial value
			out.writeBoolean(false);
		} else {
			// we have an initial value
			out.writeBoolean(true);

			byte[] serializedDefaultValue;
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(baos))
			{
				TypeSerializer<ACC> duplicateSerializer = typeSerializer.duplicate();
				duplicateSerializer.serialize(initialValue, outView);

				outView.flush();
				serializedDefaultValue = baos.toByteArray();
			}
			catch (Exception e) {
				throw new IOException("Unable to serialize initial value of type " +
					initialValue.getClass().getSimpleName() + ".", e);
			}

			out.writeInt(serializedDefaultValue.length);
			out.write(serializedDefaultValue);
		}
	}

	private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
		// read the fold function
		in.defaultReadObject();

		// read the initial value field
		boolean hasInitialValue = in.readBoolean();
		if (hasInitialValue) {
			int size = in.readInt();

			byte[] buffer = new byte[size];

			in.readFully(buffer);

			try (ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
				DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(bais))
			{
				initialValue = typeSerializer.deserialize(inView);
			}
			catch (Exception e) {
				throw new IOException("Unable to deserialize initial value.", e);
			}
		} else {
			initialValue = null;
		}
	}

	@Override
	public Type getType() {
		return Type.FOLDING;
	}
}
