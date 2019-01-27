/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base row serializer.
 */
public abstract class AbstractRowSerializer<T extends BaseRow> extends TypeSerializer<T> {

	private static final long serialVersionUID = -3420438571863908761L;

	protected final int numFields;
	protected final TypeInformation<?>[] types;
	protected final TypeSerializer[] serializers;

	public AbstractRowSerializer(TypeInformation<?>[] types) {
		this.types = checkNotNull(types);
		this.numFields = types.length;
		TypeSerializer[] fieldSerializers = new TypeSerializer[types.length];
		for (int i = 0; i < types.length; i++) {
			fieldSerializers[i] = DataTypes.createInternalSerializer(
					TypeConverters.createInternalTypeFromTypeInfo(types[i]));
		}
		this.serializers = fieldSerializers;
	}

	public int getNumFields() {
		return numFields;
	}

	public TypeInformation<?>[] getTypes() {
		return types;
	}

	public abstract BinaryRow baseRowToBinary(T baseRow) throws IOException;

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		return new RowSerializerConfigSnapshot(serializers);
	}

	@Override
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof RowSerializerConfigSnapshot) {
			TypeSerializer[] fieldSerializers = serializers;
			List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> previousFieldSerializersAndConfigs =
					((RowSerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();

			if (previousFieldSerializersAndConfigs.size() == fieldSerializers.length) {
				boolean requireMigration = false;
				TypeSerializer<?>[] convertDeserializers = new TypeSerializer<?>[fieldSerializers.length];

				CompatibilityResult<?> compatResult;
				int i = 0;
				for (Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> f : previousFieldSerializersAndConfigs) {
					compatResult = CompatibilityUtil.resolveCompatibilityResult(
							f.f0,
							UnloadableDummyTypeSerializer.class,
							f.f1,
							fieldSerializers[i]);

					if (compatResult.isRequiresMigration()) {
						requireMigration = true;

						if (compatResult.getConvertDeserializer() == null) {
							// one of the field serializers cannot provide a fallback deserializer
							return CompatibilityResult.requiresMigration();
						} else {
							convertDeserializers[i] =
									new TypeDeserializerAdapter<>(compatResult.getConvertDeserializer());
						}
					}

					i++;
				}

				if (requireMigration) {
					// TODO how to get a new RowSerializer(convertDeserializers)
					return CompatibilityResult.requiresMigration(this);
				} else {
					return CompatibilityResult.compatible();
				}
			}
		}

		return CompatibilityResult.requiresMigration();
	}

	/**
	 * Snapshot.
	 */
	public static final class RowSerializerConfigSnapshot extends CompositeTypeSerializerConfigSnapshot {

		private static final int VERSION = 1;

		/** This empty nullary constructor is required for deserializing the configuration. */
		public RowSerializerConfigSnapshot() {}

		public RowSerializerConfigSnapshot(TypeSerializer[] fieldSerializers) {
			super(fieldSerializers);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}
}
