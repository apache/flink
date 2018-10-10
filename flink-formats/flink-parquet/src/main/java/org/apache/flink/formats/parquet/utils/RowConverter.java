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

package org.apache.flink.formats.parquet.utils;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Extends from {@link GroupConverter} to convert an nested Parquet Record into Row.
 */
public class RowConverter extends GroupConverter implements ParentDataHolder {
	private static final Logger LOGGER = LoggerFactory.getLogger(RowConverter.class);
	private final Converter[] converters;
	private final ParentDataHolder parentDataHolder;
	private final TypeInformation<?> typeInfo;
	private Row currentRow;
	private int posInParentRow;

	public RowConverter(MessageType messageType, TypeInformation<?> typeInfo) {
		this(messageType, typeInfo, null, 0);
	}

	public RowConverter(GroupType schema, TypeInformation<?> typeInfo, ParentDataHolder parent, int pos) {
		this.typeInfo = typeInfo;
		this.converters = new Converter[schema.getFieldCount()];
		this.parentDataHolder = parent;
		this.posInParentRow = pos;
		int i = 0;
		if (typeInfo.getArity() >= 1 && (typeInfo instanceof CompositeType)) {
			for (Type field : schema.getFields()) {
				converters[i] = createConverter(field, i, ((CompositeType<?>) typeInfo).getTypeAt(i), this);
				i++;
			}
		}
	}

	private static Converter createConverter(
		Type field,
		int fieldPos,
		TypeInformation<?> typeInformation,
		ParentDataHolder parentDataHolder) {
		if (field.isPrimitive()) {
			return new RowConverter.RowPrimitiveConverter(field, parentDataHolder, fieldPos);
		} else if (typeInformation instanceof MapTypeInfo) {
			return new RowConverter.MapConverter((GroupType) field, (MapTypeInfo) typeInformation,
				parentDataHolder, fieldPos);
		} else if (typeInformation instanceof BasicArrayTypeInfo) {
			Type elementType = field.asGroupType().getFields().get(0);
			Class typeClass = ((BasicArrayTypeInfo) typeInformation).getComponentInfo().getTypeClass();
			if (typeClass.equals(Character.class)) {
				return new RowConverter.BasicArrayConverter<Character>((BasicArrayTypeInfo) typeInformation, elementType,
					Character.class, parentDataHolder, fieldPos);
			} else if (typeClass.equals(Boolean.class)) {
				return new RowConverter.BasicArrayConverter<Boolean>((BasicArrayTypeInfo) typeInformation, elementType,
					Boolean.class, parentDataHolder, fieldPos);
			} else if (typeClass.equals(Short.class)) {
				return new RowConverter.BasicArrayConverter<Short>((BasicArrayTypeInfo) typeInformation, elementType,
					Short.class, parentDataHolder, fieldPos);
			} else if (typeClass.equals(Integer.class)) {
				return new RowConverter.BasicArrayConverter<Integer>((BasicArrayTypeInfo) typeInformation, elementType,
					Integer.class, parentDataHolder, fieldPos);
			} else if (typeClass.equals(Long.class)) {
				return new RowConverter.BasicArrayConverter<Long>((BasicArrayTypeInfo) typeInformation, elementType,
					Long.class, parentDataHolder, fieldPos);
			} else if (typeClass.equals(Double.class)) {
				return new RowConverter.BasicArrayConverter<Double>((BasicArrayTypeInfo) typeInformation, elementType,
					Double.class, parentDataHolder, fieldPos);
			} else if (typeClass.equals(String.class)) {
				return new RowConverter.BasicArrayConverter<String>((BasicArrayTypeInfo) typeInformation, elementType,
					String.class, parentDataHolder, fieldPos);
			} else if (typeClass.equals(Date.class)) {
				return new RowConverter.BasicArrayConverter<Date>((BasicArrayTypeInfo) typeInformation, elementType,
					Date.class, parentDataHolder, fieldPos);
			} else if (typeClass.equals(Time.class)) {
				return new RowConverter.BasicArrayConverter<Time>((BasicArrayTypeInfo) typeInformation, elementType,
					Time.class, parentDataHolder, fieldPos);
			} else if (typeClass.equals(Timestamp.class)) {
				return new RowConverter.BasicArrayConverter<Timestamp>((BasicArrayTypeInfo) typeInformation,
					elementType, Timestamp.class, parentDataHolder, fieldPos);
			} else if (typeClass.equals(BigDecimal.class)) {
				return new RowConverter.BasicArrayConverter<BigDecimal>((BasicArrayTypeInfo) typeInformation,
					elementType, BigDecimal.class, parentDataHolder, fieldPos);
			}

			throw new IllegalArgumentException(
				String.format("Can't create unsupported primitive array type for %s", typeClass.toString()));

		} else if (typeInformation instanceof ObjectArrayTypeInfo) {
			return new RowConverter.ObjectArrayConverter(field, (ObjectArrayTypeInfo) typeInformation,
				parentDataHolder, fieldPos);
		} else if (typeInformation instanceof RowTypeInfo) {
			return new RowConverter((GroupType) field, typeInformation, parentDataHolder, fieldPos);
		}

		// Other are types, we don't support.
		return null;
	}

	@Override
	public Converter getConverter(int i) {
		return converters[i];
	}

	@Override
	public void start() {
		this.currentRow = new Row(typeInfo.getArity());
	}

	public Row getCurrentRow() {
		return currentRow;
	}

	@Override
	public void end() {
		if (parentDataHolder != null) {
			parentDataHolder.add(posInParentRow, currentRow);
		}
	}

	@Override
	public void add(int fieldIndex, Object object) {
		currentRow.setField(fieldIndex, object);
	}

	static class RowPrimitiveConverter extends PrimitiveConverter {
		private Type dataType;
		private OriginalType originalType;
		private PrimitiveType.PrimitiveTypeName primitiveTypeName;
		private ParentDataHolder parentDataHolder;
		private int pos;

		RowPrimitiveConverter(Type dataType, ParentDataHolder parentDataHolder, int pos) {
			this.dataType = dataType;
			this.parentDataHolder = parentDataHolder;
			this.pos = pos;
			if (dataType.isPrimitive()) {
				this.originalType = dataType.getOriginalType();
				this.primitiveTypeName = dataType.asPrimitiveType().getPrimitiveTypeName();
			} else {
				// Backward-compatibility  It can be a group type middle layer
				Type primitiveType = dataType.asGroupType().getType(0);
				this.originalType = primitiveType.getOriginalType();
				this.primitiveTypeName = primitiveType.asPrimitiveType().getPrimitiveTypeName();
			}
		}

		@Override
		public void addBinary(Binary value) {
			// in case it is a timestamp type stored as INT96
			if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.INT96)) {
				parentDataHolder.add(pos, new Timestamp(ParquetTimestampUtils.getTimestampMillis(value)));
				return;
			}

			if (originalType != null) {
				switch (originalType) {
					case DECIMAL:
						parentDataHolder.add(pos, new BigDecimal(value.toStringUsingUTF8().toCharArray()));
						break;
					case UTF8:
					case ENUM:
					case JSON:
					case BSON:
						parentDataHolder.add(pos, value.toStringUsingUTF8());
						break;
					default:
						throw new UnsupportedOperationException("Unsupported original type : " + originalType.name()
							+ " for primitive type BINARY");
				}
			} else {
				parentDataHolder.add(pos, value.toStringUsingUTF8());
			}
		}

		@Override
		public void addBoolean(boolean value) {
			parentDataHolder.add(pos, value);
		}

		@Override
		public void addDouble(double value) {
			parentDataHolder.add(pos, value);
		}

		@Override
		public void addFloat(float value) {
			parentDataHolder.add(pos, value);
		}

		@Override
		public void addInt(int value) {
			if (originalType != null) {
				switch (originalType) {
					case TIME_MICROS:
					case TIME_MILLIS:
						parentDataHolder.add(pos, new Time(value));
						break;
					case TIMESTAMP_MICROS:
					case TIMESTAMP_MILLIS:
						parentDataHolder.add(pos, new Timestamp(value));
						break;
					case DATE:
						parentDataHolder.add(pos, new Date(value));
						break;
					case UINT_8:
					case UINT_16:
					case UINT_32:
					case INT_8:
					case INT_16:
					case INT_32:
						parentDataHolder.add(pos, value);
						break;
					default:
						throw new UnsupportedOperationException("Unsupported original type : " + originalType.name()
							+ " for primitive type INT32");
				}
			} else {
				parentDataHolder.add(pos, value);
			}
		}

		@Override
		public void addLong(long value) {
			if (originalType != null) {
				switch (originalType) {
					case TIME_MICROS:
						parentDataHolder.add(pos, new Time(value));
						break;
					case TIMESTAMP_MICROS:
					case TIMESTAMP_MILLIS:
						parentDataHolder.add(pos, new Timestamp(value));
						break;
					case INT_64:
					case DECIMAL:
						// long is more efficient then BigDecimal in terms of memory.
						parentDataHolder.add(pos, value);
						break;
					default:
						throw new UnsupportedOperationException("Unsupported original type : " + originalType.name()
							+ " for primitive type INT64");
				}
			} else {
				parentDataHolder.add(pos, value);
			}
		}
	}

	static class ObjectArrayConverter<T extends RowTypeInfo> extends GroupConverter implements ParentDataHolder {
		private final ParentDataHolder parentDataHolder;
		private final ObjectArrayTypeInfo objectArrayTypeInfo;
		private final Converter elementConverter;
		private final Type type;
		private final int pos;
		private List<Row> list;

		ObjectArrayConverter(Type type, ObjectArrayTypeInfo typeInfo, ParentDataHolder parentDataHolder, int pos) {
			this.type = type;
			this.parentDataHolder = parentDataHolder;
			this.objectArrayTypeInfo = typeInfo;
			this.pos = pos;
			GroupType parquetGroupType = type.asGroupType();
			Type elementType = parquetGroupType.getType(0);
			this.elementConverter = createConverter(elementType, 0, objectArrayTypeInfo.getComponentInfo(), this);
		}

		@Override
		public Converter getConverter(int fieldIndex) {
			return elementConverter;
		}

		@Override
		public void start() {
			list = new ArrayList<>();
		}

		@Override
		public void end() {
			parentDataHolder.add(pos, list.toArray((Row[]) Array.newInstance(Row.class, list.size())));
		}

		@Override
		public void add(int fieldIndex, Object object) {
			list.add((Row) object);
		}
	}

	@SuppressWarnings("unchecked")
	static class BasicArrayConverter<T> extends GroupConverter implements ParentDataHolder {
		private final ParentDataHolder parentDataHolder;
		private final BasicArrayTypeInfo typeInfo;
		private final Type elementType;
		private final Class elementClass;
		private final int pos;
		private List<T> list;
		private Converter elementConverter;

		BasicArrayConverter(BasicArrayTypeInfo typeInfo, Type elementType, Class primitiveClass,
							ParentDataHolder parentDataHolder, int pos) {
			this.typeInfo = typeInfo;
			this.elementType = elementType;
			this.elementClass = primitiveClass;
			this.parentDataHolder = parentDataHolder;
			this.pos = pos;
			elementConverter = new RowConverter.RowPrimitiveConverter(elementType, this, 0);
		}

		@Override
		public Converter getConverter(int fieldIndex) {
			return elementConverter;
		}

		@Override
		public void start() {
			list = new ArrayList<>();
		}

		@Override
		public void end() {
			parentDataHolder.add(pos, list.toArray((T[]) Array.newInstance(elementClass, list.size())));
		}

		@Override
		public void add(int fieldIndex, Object object) {
			list.add((T) object);
		}
	}

	static class MapConverter extends GroupConverter {
		private final ParentDataHolder parentDataHolder;
		private final Converter keyValueConverter;
		private final MapTypeInfo typeInfo;
		private final int pos;
		private Map<Object, Object> map;

		MapConverter(GroupType type, MapTypeInfo typeInfo, ParentDataHolder parentDataHolder, int pos) {
			this.parentDataHolder = parentDataHolder;
			this.typeInfo = typeInfo;
			this.pos = pos;
			this.keyValueConverter = new MapKeyValueConverter((GroupType) type.getType(0), typeInfo);
		}

		@Override
		public Converter getConverter(int fieldIndex) {
			return keyValueConverter;
		}

		@Override
		public void start() {
			map = new HashMap<>();
		}

		@Override
		public void end() {
			parentDataHolder.add(pos, map);
		}

		final class MapKeyValueConverter extends GroupConverter {
			private final Converter keyConverter;
			private final Converter valueConverter;
			private Object key;
			private Object value;

			MapKeyValueConverter(GroupType groupType, MapTypeInfo typeInformation) {
				this.keyConverter = createConverter(groupType.getType(0), 0,
					typeInformation.getKeyTypeInfo(), new ParentDataHolder() {
						@Override
						public void add(int fieldIndex, Object object) {
							key = object;
						}
					});

				this.valueConverter = createConverter(groupType.getType(1), 1,
					typeInformation.getValueTypeInfo(), new ParentDataHolder() {
						@Override
						public void add(int fieldIndex, Object object) {
							value = object;
						}
					});
			}

			@Override
			public Converter getConverter(int fieldIndex) {
				if (fieldIndex == 0) {
					return keyConverter;
				} else {
					return valueConverter;
				}
			}

			@Override
			public void start() {
				key = null;
				value = null;
			}

			@Override
			public void end() {
				map.put(this.key, this.value);
			}
		}
	}
}
