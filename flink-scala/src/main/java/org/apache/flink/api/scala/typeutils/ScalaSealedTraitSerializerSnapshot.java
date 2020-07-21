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
package org.apache.flink.api.scala.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * A {@link TypeSerializerSnapshot} for the Scala {@link SealedTraitSerializer}.
 */
public class ScalaSealedTraitSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

	public static final int VERSION = 1;

	Class<?>[] subtypeClasses;
	TypeSerializer<?>[] subtypeSerializers;

	/**
	 * Snapshot read constructor
	 */
	public ScalaSealedTraitSerializerSnapshot() {}

	/**
	 * Snapshot write constructor
	 * @param instance the serializer instance to snapshot
	 */
	public ScalaSealedTraitSerializerSnapshot(SealedTraitSerializer<T> instance) {
		this.subtypeClasses = instance.subtypeClasses();
		this.subtypeSerializers = instance.subtypeSerializers();
	}

	@Override
	public int getCurrentVersion() {
		return VERSION;
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		out.writeInt(VERSION);
		out.writeInt(subtypeClasses.length);
		for (Class<?> clazz: subtypeClasses) {
			out.writeUTF(clazz.getName());
		}
		for (TypeSerializer<?> ser: subtypeSerializers) {
			TypeSerializerSnapshot.writeVersionedSnapshot(out, ser.snapshotConfiguration());
		}
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		int version = in.readInt();
		int subtypes = in.readInt();
		subtypeClasses = new Class<?>[subtypes];
		for (int i = 0; i < subtypes; i++) {
			subtypeClasses[i] = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);
		}
		subtypeSerializers = new TypeSerializer<?>[subtypes];
		for (int i = 0; i < subtypes; i++) {
			subtypeSerializers[i] = TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader).restoreSerializer();
		}
	}

	@Override
	public TypeSerializer<T> restoreSerializer() {
		return new SealedTraitSerializer<T>(subtypeClasses, subtypeSerializers);
	}

	@Override
	public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
		if (newSerializer instanceof SealedTraitSerializer) {
			SealedTraitSerializer<T> sealed = (SealedTraitSerializer<T>) newSerializer;
			// we cannot remove ADT members, so it's expected that ADT size can only grow
			if (sealed.subtypeClasses().length >= subtypeClasses.length) {
				boolean compatible = true;
				// ADT members can be added only by appending them
				for (int i = 0; (i < subtypeClasses.length) && compatible; i++) {
					if (subtypeClasses[i] != sealed.subtypeClasses()[i]) compatible = false;
				}
				if (compatible) {
					if (subtypeClasses.length == sealed.subtypeClasses().length) {
						return TypeSerializerSchemaCompatibility.compatibleAsIs();
					} else {
						// there are new members added, so old serializer cannot be used for writing
						return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
					}
				}
			}
		}
		return TypeSerializerSchemaCompatibility.incompatible();
	}
}
