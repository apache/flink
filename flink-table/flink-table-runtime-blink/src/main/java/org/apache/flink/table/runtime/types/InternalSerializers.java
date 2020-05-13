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

package org.apache.flink.table.runtime.types;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.table.runtime.typeutils.ArrayDataSerializer;
import org.apache.flink.table.runtime.typeutils.DecimalDataSerializer;
import org.apache.flink.table.runtime.typeutils.MapDataSerializer;
import org.apache.flink.table.runtime.typeutils.RawValueDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.StringDataSerializer;
import org.apache.flink.table.runtime.typeutils.TimestampDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.TypeInformationRawType;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;

/**
 * {@link TypeSerializer} of {@link LogicalType} for internal sql engine execution data formats.
 */
public class InternalSerializers {

	/**
	 * Creates a {@link TypeSerializer} for internal data structures of the given {@link LogicalType}.
	 */
	public static TypeSerializer<?> create(LogicalType type) {
		return create(type, new ExecutionConfig());
	}

	/**
	 * Creates a {@link TypeSerializer} for internal data structures of the given {@link LogicalType}.
	 */
	public static TypeSerializer create(LogicalType type, ExecutionConfig config) {
		// ordered by type root definition
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				return StringDataSerializer.INSTANCE;
			case BOOLEAN:
				return BooleanSerializer.INSTANCE;
			case BINARY:
			case VARBINARY:
				return BytePrimitiveArraySerializer.INSTANCE;
			case DECIMAL:
				return new DecimalDataSerializer(getPrecision(type), getScale(type));
			case TINYINT:
				return ByteSerializer.INSTANCE;
			case SMALLINT:
				return ShortSerializer.INSTANCE;
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				return IntSerializer.INSTANCE;
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return LongSerializer.INSTANCE;
			case FLOAT:
				return FloatSerializer.INSTANCE;
			case DOUBLE:
				return DoubleSerializer.INSTANCE;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return new TimestampDataSerializer(getPrecision(type));
			case TIMESTAMP_WITH_TIME_ZONE:
				throw new UnsupportedOperationException();
			case ARRAY:
				return new ArrayDataSerializer(((ArrayType) type).getElementType(), config);
			case MULTISET:
				return new MapDataSerializer(((MultisetType) type).getElementType(), new IntType(false), config);
			case MAP:
				MapType mapType = (MapType) type;
				return new MapDataSerializer(mapType.getKeyType(), mapType.getValueType(), config);
			case ROW:
			case STRUCTURED_TYPE:
				return new RowDataSerializer(config, type.getChildren().toArray(new LogicalType[0]));
			case DISTINCT_TYPE:
				return create(((DistinctType) type).getSourceType(), config);
			case RAW:
				if (type instanceof RawType) {
					final RawType<?> rawType = (RawType<?>) type;
					return new RawValueDataSerializer<>(rawType.getTypeSerializer());
				}
				return new RawValueDataSerializer<>(
					((TypeInformationRawType<?>) type).getTypeInformation().createSerializer(config));
			case NULL:
			case SYMBOL:
			case UNRESOLVED:
			default:
				throw new UnsupportedOperationException(
					"Unsupported type '" + type + "' to get internal serializer");
		}
	}
}
