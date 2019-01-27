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

package org.apache.flink.table.sources.orc;

import org.apache.flink.table.api.types.BooleanType;
import org.apache.flink.table.api.types.ByteType;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DateType;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.DoubleType;
import org.apache.flink.table.api.types.FloatType;
import org.apache.flink.table.api.types.IntType;
import org.apache.flink.table.api.types.LongType;
import org.apache.flink.table.api.types.ShortType;
import org.apache.flink.table.api.types.StringType;
import org.apache.flink.table.api.types.TimestampType;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcTimestamp;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.serde2.io.DateWritable;
import org.apache.orc.storage.serde2.io.HiveDecimalWritable;

import java.util.HashMap;
import java.util.Map;

/**
 * A deserializer to deserialize ORC structs to flink rows.
 */

public class OrcDeserializer {

	private final DataType[] fieldTypes;

	private final String[] fieldNames;

	private final int[] columnIds;

	private final Converter[] fieldConverters;

	private static final Map<Class<? extends DataType>, Converter> CONVERTER_MAP =
			new HashMap<Class<? extends DataType>, Converter>() {
				private static final long serialVersionUID = 2137301835386882070L;
				{
					put(BooleanType.class, new BooleanConverter());
					put(ByteType.class, new ByteConverter());
					put(ShortType.class, new ShortConverter());
					put(IntType.class, new IntConverter());
					put(LongType.class, new LongConverter());
					put(FloatType.class, new FloatConverter());
					put(DoubleType.class, new DoubleConverter());
					put(StringType.class, new StringConverter());
					put(DateType.class, new SqlDateConverter());
					put(TimestampType.class, new SqlTimestampConverter());
					put(DecimalType.class, new DecimalConverter());
				}
			};

	public OrcDeserializer(DataType[] fieldTypes, String[] fieldNames, int[] columnIds) {
		Preconditions.checkArgument(fieldTypes != null && fieldTypes.length > 0);
		Preconditions.checkArgument(fieldNames != null && fieldNames.length == fieldTypes.length);

		this.fieldTypes = fieldTypes;
		this.fieldNames = fieldNames;
		this.columnIds = columnIds;

		fieldConverters = new Converter[fieldTypes.length];

		for (int i = 0; i < fieldTypes.length; i++) {
			fieldConverters[i] = CONVERTER_MAP.get(fieldTypes[i].getClass());
			fieldConverters[i].setFieldType(fieldTypes[i]);
		}
	}

	public Row deserialize(OrcStruct orcStruct, Row reuse) {
		for (int i = 0; i < reuse.getArity(); i++) {
			reuse.setField(i, null);
		}

		for (int i = 0; i < columnIds.length; i++) {
			if (columnIds[i] != -1) {
				WritableComparable value = orcStruct.getFieldValue(columnIds[i]);
				if (value == null) {
					reuse.setField(i, null);
				} else {
					reuse.setField(i, fieldConverters[i].convert(value));
				}
			}
		}

		return reuse;
	}

	private abstract static class Converter {
		protected DataType fieldType;

		protected abstract Object convert(WritableComparable value);

		protected void setFieldType(DataType fieldType) {
			this.fieldType = fieldType;
		}
	}

	private static class StringConverter extends Converter {
		@Override
		protected Object convert(WritableComparable value) {
			Preconditions.checkArgument(value instanceof Text);
			return ((Text) value).toString();
		}
	}

	private static class BooleanConverter extends Converter {
		@Override
		protected Object convert(WritableComparable value) {
			Preconditions.checkArgument(value instanceof BooleanWritable);
			return ((BooleanWritable) value).get();
		}
	}

	private static class ByteConverter extends Converter {
		@Override
		protected Object convert(WritableComparable value) {
			Preconditions.checkArgument(value instanceof ByteWritable);
			return ((ByteWritable) value).get();
		}
	}

	private static class ShortConverter extends Converter {
		@Override
		protected Object convert(WritableComparable value) {
			Preconditions.checkArgument(value instanceof ShortWritable);
			return ((ShortWritable) value).get();
		}
	}

	private static class IntConverter extends Converter {
		@Override
		protected Object convert(WritableComparable value) {
			Preconditions.checkArgument(value instanceof IntWritable);
			return ((IntWritable) value).get();
		}
	}

	private static class LongConverter extends Converter {
		@Override
		protected Object convert(WritableComparable value) {
			Preconditions.checkArgument(value instanceof LongWritable);
			return ((LongWritable) value).get();
		}
	}

	private static class FloatConverter extends Converter {
		@Override
		protected Object convert(WritableComparable value) {
			Preconditions.checkArgument(value instanceof FloatWritable);
			return ((FloatWritable) value).get();
		}
	}

	private static class DoubleConverter extends Converter {
		@Override
		protected Object convert(WritableComparable value) {
			Preconditions.checkArgument(value instanceof DoubleWritable);
			return ((DoubleWritable) value).get();
		}
	}

	private static class DecimalConverter extends Converter {
		@Override
		protected Object convert(WritableComparable value) {
			Preconditions.checkArgument(value instanceof HiveDecimalWritable);
			HiveDecimal decimal = ((HiveDecimalWritable) value).getHiveDecimal();
			int precision = ((DecimalType) fieldType).precision();
			int scale = ((DecimalType) fieldType).scale();
			return Decimal.fromBigDecimal(decimal.bigDecimalValue(), precision, scale).toBigDecimal();
		}
	}

	private static class SqlDateConverter extends Converter {
		@Override
		protected Object convert(WritableComparable value) {
			Preconditions.checkArgument(value instanceof DateWritable);
			return BuildInScalarFunctions.internalToDate(((DateWritable) value).getDays());
		}
	}

	private static class SqlTimestampConverter extends Converter {
		@Override
		protected Object convert(WritableComparable value) {
			Preconditions.checkArgument(value instanceof OrcTimestamp);
			return BuildInScalarFunctions.internalToTimestamp(((OrcTimestamp) value).getTime());
		}
	}
}
