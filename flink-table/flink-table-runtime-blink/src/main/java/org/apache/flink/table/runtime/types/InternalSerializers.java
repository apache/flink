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
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TypeInformationRawType;

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
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return BooleanSerializer.INSTANCE;
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
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				TimestampType timestampType = (TimestampType) type;
				return new TimestampDataSerializer(timestampType.getPrecision());
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				LocalZonedTimestampType lzTs = (LocalZonedTimestampType) type;
				return new TimestampDataSerializer(lzTs.getPrecision());
			case FLOAT:
				return FloatSerializer.INSTANCE;
			case DOUBLE:
				return DoubleSerializer.INSTANCE;
			case CHAR:
			case VARCHAR:
				return StringDataSerializer.INSTANCE;
			case DECIMAL:
				DecimalType decimalType = (DecimalType) type;
				return new DecimalDataSerializer(decimalType.getPrecision(), decimalType.getScale());
			case ARRAY:
				return new ArrayDataSerializer(((ArrayType) type).getElementType(), config);
			case MAP:
				MapType mapType = (MapType) type;
				return new MapDataSerializer(mapType.getKeyType(), mapType.getValueType(), config);
			case MULTISET:
				return new MapDataSerializer(((MultisetType) type).getElementType(), new IntType(), config);
			case ROW:
				RowType rowType = (RowType) type;
				return new RowDataSerializer(config, rowType);
			case BINARY:
			case VARBINARY:
				return BytePrimitiveArraySerializer.INSTANCE;
			case RAW:
				if (type instanceof RawType) {
					final RawType<?> rawType = (RawType<?>) type;
					return new RawValueDataSerializer<>(rawType.getTypeSerializer());
				}
				return new RawValueDataSerializer<>(
					((TypeInformationRawType<?>) type).getTypeInformation().createSerializer(config));
			default:
				throw new UnsupportedOperationException(
					"Unsupported type '" + type + "' to get internal serializer");
		}
	}
}
