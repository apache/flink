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
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A serializer to serialize Flink rows to ORC structs.
 */
public class OrcSerializer implements Serializable {

	private final DataType[] fieldTypes;

	private final String[] fieldNames;

	private final Converter[] fieldConverters;

	public OrcSerializer(DataType[] fieldTypes, String[] fieldNames) {
		Preconditions.checkArgument(fieldTypes != null && fieldTypes.length > 0);
		Preconditions.checkArgument(fieldNames != null && fieldNames.length == fieldTypes.length);
		this.fieldTypes = fieldTypes;
		this.fieldNames = fieldNames;

		fieldConverters = new Converter[this.fieldTypes.length];
		for (int i = 0; i < this.fieldTypes.length; i++) {
			fieldConverters[i] = CONVERTER_MAP.get(this.fieldTypes[i].getClass());
		}
	}

	private static final Map<Class<? extends DataType>, Converter> CONVERTER_MAP =
		new HashMap<Class<? extends DataType>, Converter>() {
			private static final long serialVersionUID = 4338806462093593810L;

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

	public OrcStruct serialize(BaseRow row, OrcStruct struct) {
		for (int i = 0; i < fieldConverters.length; i++) {
			if (row.isNullAt(i)) {
				struct.setFieldValue(i, null);
			} else {
				struct.setFieldValue(i, fieldConverters[i].convert(BaseRowUtil.get(row, i, fieldTypes[i])));
			}
		}
		return struct;
	}

	private abstract static class Converter implements Serializable {
		protected abstract WritableComparable convert(Object value);
	}

	private static class BooleanConverter extends Converter {
		@Override
		protected WritableComparable convert(Object value) {
			return new BooleanWritable((boolean) value);
		}
	}

	private static class ByteConverter extends Converter {
		@Override
		protected WritableComparable convert(Object value) {
			return new ByteWritable((byte) value);
		}
	}

	private static class ShortConverter extends Converter {
		@Override
		protected WritableComparable convert(Object value) {
			return new ShortWritable((short) value);
		}
	}

	private static class IntConverter extends Converter {
		@Override
		protected WritableComparable convert(Object value) {
			return new IntWritable((int) value);
		}
	}

	private static class LongConverter extends Converter {
		@Override
		protected WritableComparable convert(Object value) {
			return new LongWritable((long) value);
		}
	}

	private static class FloatConverter extends Converter {
		@Override
		protected WritableComparable convert(Object value) {
			return new FloatWritable((float) value);
		}
	}

	private static class DoubleConverter extends Converter {
		@Override
		protected WritableComparable convert(Object value) {
			return new DoubleWritable((double) value);
		}
	}

	private static class StringConverter extends Converter {
		@Override
		protected WritableComparable convert(Object value) {
			if (value instanceof BinaryString) {
				return new Text(((BinaryString) value).getBytes());
			} else if (value instanceof String) {
				return new Text((String) value);
			} else {
				throw new RuntimeException("Unsupport type: " + value);
			}
		}
	}

	private static class DecimalConverter extends Converter {
		@Override
		protected WritableComparable convert(Object value) {
			return new HiveDecimalWritable(HiveDecimal.create(((Decimal) value).toBigDecimal()));
		}
	}

	private static class SqlDateConverter extends Converter {
		@Override
		protected WritableComparable convert(Object value) {
			return new DateWritable((int) value);
		}
	}

	private static class SqlTimestampConverter extends Converter {
		@Override
		protected WritableComparable convert(Object value) {
			return new OrcTimestamp((long) value);
		}
	}
}
