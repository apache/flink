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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link TypeSerializerConfigSnapshot} for serializers that has multiple nested serializers.
 * The configuration snapshot consists of the configuration snapshots of all nested serializers, and
 * also the nested serializers themselves.
 *
 * <p>Both the nested serializers and the configuration snapshots are written as configuration of
 * composite serializers, so that on restore, the previous serializer may be used in case migration
 * is required.
 */
@Internal
public abstract class CompositeTypeSerializerConfigSnapshot<T> extends TypeSerializerConfigSnapshot<T> {

	private List<TypeSerializerConfigSnapshot<?>> nestedSerializerConfigs;

	/** This empty nullary constructor is required for deserializing the configuration. */
	public CompositeTypeSerializerConfigSnapshot() {}

	public CompositeTypeSerializerConfigSnapshot(TypeSerializer<?>... nestedSerializers) {
		Preconditions.checkNotNull(nestedSerializers);

		this.nestedSerializerConfigs = new ArrayList<>(nestedSerializers.length);
		for (TypeSerializer<?> nestedSerializer : nestedSerializers) {
			this.nestedSerializerConfigs.add(
				Preconditions.checkNotNull(
					nestedSerializer.snapshotConfiguration(),
					"Configuration snapshots of nested serializers cannot be null."));
		}
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
		TypeSerializerConfigSnapshotSerializationUtil.writeSerializerConfigSnapshots(out, nestedSerializerConfigs);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		if (!containsSerializers()) {
			this.nestedSerializerConfigs = TypeSerializerConfigSnapshotSerializationUtil
				.readSerializerConfigSnapshots(in, getUserCodeClassLoader());
		} else {
			// backwards compatible path
			List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> nestedSerializersAndConfigs =
				TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, getUserCodeClassLoader());

			this.nestedSerializerConfigs = new ArrayList<>(nestedSerializersAndConfigs.size());
			for (Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> entry : nestedSerializersAndConfigs) {
				this.nestedSerializerConfigs.add(new BackwardsCompatibleConfigSnapshot<>(entry.f1, entry.f0));
			}
		}
	}

	/**
	 * Return whether or not this composite type serializer config snapshot still contains
	 * serializers. Subclasses should uptick their version, and use that to compare against the read version
	 * of the config snapshot to determine this. By default, it is assumed that all composite
	 * type serializer config snapshots do not contain serializers.
	 */
	protected boolean containsSerializers() {
		return false;
	}

	public int getNumNestedSerializers() {
		return nestedSerializerConfigs.size();
	}

	public TypeSerializerConfigSnapshot<?> getNestedSerializerConfigSnapshot(int index) {
		return nestedSerializerConfigs.get(index);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}

		if (obj == null) {
			return false;
		}

		return (obj.getClass().equals(getClass()))
				&& nestedSerializerConfigs.equals(((CompositeTypeSerializerConfigSnapshot) obj).nestedSerializerConfigs);
	}

	@Override
	public int hashCode() {
		return nestedSerializerConfigs.hashCode();
	}

	@Override
	public final TypeSerializer<T> restoreSerializer() {
		TypeSerializer<?>[] restoredNestedSerializers = new TypeSerializer[nestedSerializerConfigs.size()];

		int i = 0;
		for (TypeSerializerConfigSnapshot<?> config : nestedSerializerConfigs) {
			restoredNestedSerializers[i] = config.restoreSerializer();
			i++;
		}

		return restoreSerializer(restoredNestedSerializers);
	}

	/**
	 * Restore the composite type serializer with the restored nested serializers.
	 *
	 * @param restoredNestedSerializers the restored nested serializers.
	 * @return the restored composite type serializer.
	 */
	protected abstract TypeSerializer<T> restoreSerializer(TypeSerializer<?>... restoredNestedSerializers);
}
