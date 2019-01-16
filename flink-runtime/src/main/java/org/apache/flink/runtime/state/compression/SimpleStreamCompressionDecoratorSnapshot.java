/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.compression;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Class for Flink built-in compression types.
 */
@Internal
public abstract class SimpleStreamCompressionDecoratorSnapshot implements StreamCompressionDecoratorSnapshot {

	private static final int CURRENT_VERSION = 1;

	private Class<? extends StreamCompressionDecorator> compressionDecoratorClass;

	/**
	 * Default constructor for instantiation on restore (reading the snapshot).
	 */
	@SuppressWarnings("unused")
	public SimpleStreamCompressionDecoratorSnapshot() {}

	/**
	 * Constructor to create snapshot from serializer (writing the snapshot).
	 */
	public SimpleStreamCompressionDecoratorSnapshot(@Nonnull Class<? extends StreamCompressionDecorator> serializerClass) {
		this.compressionDecoratorClass = checkNotNull(serializerClass);
	}

	@Override
	public int getCurrentVersion() {
		return CURRENT_VERSION;
	}

	@Override
	public StreamCompressionDecorator getStreamCompressionDecorator() {
		checkState(compressionDecoratorClass != null);
		return InstantiationUtil.instantiate(compressionDecoratorClass);
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		checkState(compressionDecoratorClass != null);
		out.writeUTF(compressionDecoratorClass.getName());
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		Preconditions.checkState(readVersion == CURRENT_VERSION, "Unrecognized version:" + readVersion);
		final String className = in.readUTF();
		final Class<?> clazz = resolveClassName(className, userCodeClassLoader, false);
		this.compressionDecoratorClass = cast(clazz);
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
				"Failed to read CompressionType: StreamCompressionDecorator class not found: " + className, e);
		}
	}

	@SuppressWarnings("unchecked")
	private static Class<? extends StreamCompressionDecorator> cast(Class clazz) throws IOException {
		if (!StreamCompressionDecorator.class.isAssignableFrom(clazz)) {
			throw new IOException("Failed to read CompressionType. " +
				"StreamCompressionDecorator class name leads to a class that is not a StreamCompressionDecorator: " + clazz.getName());
		}

		return (Class<? extends StreamCompressionDecorator>) clazz;
	}

	private static String guessClassNameFromCanonical(String className) {
		int lastDot = className.lastIndexOf('.');
		if (lastDot > 0 && lastDot < className.length() - 1) {
			return className.substring(0, lastDot) + '$' + className.substring(lastDot + 1);
		} else {
			return className;
		}
	}
}
