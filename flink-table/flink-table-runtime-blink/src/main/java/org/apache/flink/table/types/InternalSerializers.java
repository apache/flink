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

package org.apache.flink.table.types;

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
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TypeInformationAnyType;
import org.apache.flink.table.typeutils.BaseRowSerializer;
import org.apache.flink.table.typeutils.BinaryArraySerializer;
import org.apache.flink.table.typeutils.BinaryGenericSerializer;
import org.apache.flink.table.typeutils.BinaryMapSerializer;
import org.apache.flink.table.typeutils.BinaryStringSerializer;
import org.apache.flink.table.typeutils.DecimalSerializer;

/**
 * {@link TypeSerializer} of {@link LogicalType} for internal sql engine execution data formats.
 */
public class InternalSerializers {

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
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case INTERVAL_DAY_TIME:
				return LongSerializer.INSTANCE;
			case FLOAT:
				return FloatSerializer.INSTANCE;
			case DOUBLE:
				return DoubleSerializer.INSTANCE;
			case VARCHAR:
				return BinaryStringSerializer.INSTANCE;
			case DECIMAL:
				DecimalType decimalType = (DecimalType) type;
				return new DecimalSerializer(decimalType.getPrecision(), decimalType.getScale());
			case ARRAY:
				return BinaryArraySerializer.INSTANCE;
			case MAP:
			case MULTISET:
				return BinaryMapSerializer.INSTANCE;
			case ROW:
				RowType rowType = (RowType) type;
				return new BaseRowSerializer(config, rowType);
			case VARBINARY:
				return BytePrimitiveArraySerializer.INSTANCE;
			case ANY:
				return new BinaryGenericSerializer(
						((TypeInformationAnyType) type).getTypeInformation().createSerializer(config));
			default:
				throw new RuntimeException("Not support type: " + type);
		}
	}
}
