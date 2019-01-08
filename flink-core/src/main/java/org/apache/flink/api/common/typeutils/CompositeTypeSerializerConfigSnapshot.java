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
public abstract class CompositeTypeSerializerConfigSnapshot extends TypeSerializerConfigSnapshot {

	private List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> nestedSerializersAndConfigs;

	/** This empty nullary constructor is required for deserializing the configuration. */
	public CompositeTypeSerializerConfigSnapshot() {}

	public CompositeTypeSerializerConfigSnapshot(TypeSerializer<?>... nestedSerializers) {
		Preconditions.checkNotNull(nestedSerializers);

		this.nestedSerializersAndConfigs = new ArrayList<>(nestedSerializers.length);
		for (TypeSerializer<?> nestedSerializer : nestedSerializers) {
			TypeSerializerConfigSnapshot configSnapshot = nestedSerializer.snapshotConfiguration();
			this.nestedSerializersAndConfigs.add(
				new Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>(
					nestedSerializer.duplicate(),
					Preconditions.checkNotNull(configSnapshot)));
		}
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
		TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(out, nestedSerializersAndConfigs);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
		this.nestedSerializersAndConfigs =
			TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, getUserCodeClassLoader());
	}

	public List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> getNestedSerializersAndConfigs() {
		return nestedSerializersAndConfigs;
	}

	public Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> getSingleNestedSerializerAndConfig() {
		return nestedSerializersAndConfigs.get(0);
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
				&& nestedSerializersAndConfigs.equals(((CompositeTypeSerializerConfigSnapshot) obj).getNestedSerializersAndConfigs());
	}

	@Override
	public int hashCode() {
		return nestedSerializersAndConfigs.hashCode();
	}
}
