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

package org.apache.flink.modelserving.java.server.typeschema;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.modelserving.java.model.Model;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * Type serializer snapshot for model - used by Flink checkpointing.
 * See https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/common/typeutils/SimpleTypeSerializerSnapshot.java
 */
public class ModelSerializerConfigSnapshot<RECORD, RESULT>
	implements TypeSerializerSnapshot<Model<RECORD, RESULT>> {

    // Version
	private static final int VERSION = 1;

    // Serializer
	private Class<ModelTypeSerializer> serializerClass = ModelTypeSerializer.class;

	/**
	 * Get current snapshot version.
	 * @return snapshot version.
	 */
	@Override
	public int getCurrentVersion() {
		return VERSION;
	}

	/**
	 * write snapshot.
	 * @param out output stream.
	 */
	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		out.writeUTF(serializerClass.getName());
	}

	/**
	 * Read snapshot.
	 * @param readVersion snapshot version.
	 * @param in input stream.
	 * @param classLoader current classloader.
	 */
	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader) throws IOException {
		switch (readVersion) {
			case VERSION:
				read(in, classLoader);
				break;
				default:
					throw new IOException("Unrecognized version: " + readVersion);
		}
	}

	/**
	 * Restore serializer.
	 * @return type serializer.
	 */
	@Override
	public TypeSerializer<Model<RECORD, RESULT>> restoreSerializer() {
		return InstantiationUtil.instantiate(serializerClass);
	}

	/**
	 * Resolve serializer compatibility.
	 * @param newSerializer serializer to compare.
	 * @return compatibility resilt.
	 */
	@Override
	public TypeSerializerSchemaCompatibility<Model<RECORD, RESULT>> resolveSchemaCompatibility(TypeSerializer<Model<RECORD, RESULT>> newSerializer) {
		return newSerializer.getClass() == serializerClass ?
			TypeSerializerSchemaCompatibility.compatibleAsIs() :
			TypeSerializerSchemaCompatibility.incompatible();
	}

	/**
	 * Read serializer from input stream.
	 * @param in input stream.
	 * @param classLoader current classloader.
	 */
	private void read(DataInputView in, ClassLoader classLoader) throws IOException {
		final String className = in.readUTF();
		this.serializerClass = cast(resolveClassName(className, classLoader, false));
	}

	/**
	 * Support method to resolve class name.
	 * @param className class name.
	 * @param cl class loader.
	 * @param allowCanonicalName allow canonical name flag.
	 * @return class.
	 */
	private static Class<?> resolveClassName(String className, ClassLoader cl, boolean allowCanonicalName) throws IOException {
		try {
			return Class.forName(className, false, cl);
		}
		catch (Throwable e) {
			throw new IOException("Failed to read SimpleTypeSerializerSnapshot: Serializer class not found: " + className, e);
		}
	}

	/**
	 * Cast to required class.
	 * @param clazz class to cast.
	 * @return class of required type.
	 */
	private static  Class<ModelTypeSerializer> cast(Class<?> clazz) throws IOException {
		if (!ModelTypeSerializer.class.isAssignableFrom(clazz)) {
			throw new IOException("Failed to read SimpleTypeSerializerSnapshot. " +
				"Serializer class name leads to a class that is not a TypeSerializer: " + clazz.getName());
		}
		return (Class<ModelTypeSerializer>) clazz;
	}
}
