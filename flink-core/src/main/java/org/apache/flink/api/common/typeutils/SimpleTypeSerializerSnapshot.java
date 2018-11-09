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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A simple base class for TypeSerializerSnapshots, for serializers that have no
 * parameters. The serializer is defined solely by its class name.
 *
 * <p>Serializers that produce these snapshots must be public, have public a zero-argument
 * constructor and cannot be a non-static inner classes.
 */
@PublicEvolving
public abstract class SimpleTypeSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

	/**
	 * This snapshot starts from version 2 (since Flink 1.7.x), so that version 1 is reserved for implementing
	 * backwards compatible code paths in case we decide to make this snapshot backwards compatible with
	 * the {@link ParameterlessTypeSerializerConfig}.
	 */
	private static final int CURRENT_VERSION = 2;

	/** The class of the serializer for this snapshot.
	 * The field is null if the serializer was created for read and has not been read, yet. */
	@Nullable
	private Class<? extends TypeSerializer<T>> serializerClass;

	/**
	 * Default constructor for instantiation on restore (reading the snapshot).
	 */
	@SuppressWarnings("unused")
	public SimpleTypeSerializerSnapshot() {}

	/**
	 * Constructor to create snapshot from serializer (writing the snapshot).
	 */
	public SimpleTypeSerializerSnapshot(@Nonnull Class<? extends TypeSerializer<T>> serializerClass) {
		this.serializerClass = checkNotNull(serializerClass);
	}

	// ------------------------------------------------------------------------
	//  Serializer Snapshot Methods
	// ------------------------------------------------------------------------

	@Override
	public int getCurrentVersion() {
		return CURRENT_VERSION;
	}

	@Override
	public TypeSerializer<T> restoreSerializer() {
		checkState(serializerClass != null);
		return InstantiationUtil.instantiate(serializerClass);
	}

	@Override
	public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {

		checkState(serializerClass != null);
		return newSerializer.getClass() == serializerClass ?
				TypeSerializerSchemaCompatibility.compatibleAsIs() :
				TypeSerializerSchemaCompatibility.incompatible();
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		checkState(serializerClass != null);
		out.writeUTF(serializerClass.getName());
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader) throws IOException {
		switch (readVersion) {
			case 2:
				read(in, classLoader);
				break;
			default:
				throw new IOException("Unrecognized version: " + readVersion);
		}
	}

	private void read(DataInputView in, ClassLoader classLoader) throws IOException {
		final String className = in.readUTF();
		final Class<?> clazz = resolveClassName(className, classLoader, false);
		this.serializerClass = cast(clazz);
	}

	// ------------------------------------------------------------------------
	//  standard utilities
	// ------------------------------------------------------------------------

	@Override
	public final boolean equals(Object obj) {
		return obj != null && obj.getClass() == getClass();
	}

	@Override
	public final int hashCode() {
		return getClass().hashCode();
	}

	@Override
	public String toString() {
		return getClass().getName();
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private static Class<?> resolveClassName(String className, ClassLoader cl, boolean allowCanonicalName) throws IOException {
		try {
			return Class.forName(className, false, cl);
		}
		catch (ClassNotFoundException e) {
			if (allowCanonicalName) {
				try {
					return Class.forName(guessClassNameFromCanonical(className), false, cl);
				}
				catch (ClassNotFoundException ignored) {}
			}

			// throw with original ClassNotFoundException
			throw new IOException(
						"Failed to read SimpleTypeSerializerSnapshot: Serializer class not found: " + className, e);
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> Class<? extends TypeSerializer<T>> cast(Class<?> clazz) throws IOException {
		if (!TypeSerializer.class.isAssignableFrom(clazz)) {
			throw new IOException("Failed to read SimpleTypeSerializerSnapshot. " +
					"Serializer class name leads to a class that is not a TypeSerializer: " + clazz.getName());
		}

		return (Class<? extends TypeSerializer<T>>) clazz;
	}

	static String guessClassNameFromCanonical(String className) {
		int lastDot = className.lastIndexOf('.');
		if (lastDot > 0 && lastDot < className.length() - 1) {
			return className.substring(0, lastDot) + '$' + className.substring(lastDot + 1);
		} else {
			return className;
		}
	}
}
