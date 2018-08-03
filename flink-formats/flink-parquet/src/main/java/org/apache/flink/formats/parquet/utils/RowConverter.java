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

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
			return new RowConverter.RowPrimitiveConverter(parentDataHolder, fieldPos);
		} else if (typeInformation instanceof MapTypeInfo) {
			return new RowConverter.MapConverter((GroupType) field, (MapTypeInfo) typeInformation,
				parentDataHolder, fieldPos);
		} else if (typeInformation instanceof BasicArrayTypeInfo) {
			Class typeClass = ((BasicArrayTypeInfo) typeInformation).getComponentInfo().getTypeClass();
			if (typeClass.equals(Character.class)) {
				return new RowConverter.BasicArrayConverter<Character>((BasicArrayTypeInfo) typeInformation, Character.class,
					parentDataHolder, fieldPos);
			} else if (typeClass.equals(Boolean.class)) {
				return new RowConverter.BasicArrayConverter<Boolean>((BasicArrayTypeInfo) typeInformation, Boolean.class,
					parentDataHolder, fieldPos);
			} else if (typeClass.equals(Short.class)) {
				return new RowConverter.BasicArrayConverter<Short>((BasicArrayTypeInfo) typeInformation, Short.class,
					parentDataHolder, fieldPos);
			} else if (typeClass.equals(Integer.class)) {
				return new RowConverter.BasicArrayConverter<Integer>((BasicArrayTypeInfo) typeInformation, Integer.class,
					parentDataHolder, fieldPos);
			} else if (typeClass.equals(Long.class)) {
				return new RowConverter.BasicArrayConverter<Long>((BasicArrayTypeInfo) typeInformation, Long.class,
					parentDataHolder, fieldPos);
			} else if (typeClass.equals(Double.class)) {
				return new RowConverter.BasicArrayConverter<Double>((BasicArrayTypeInfo) typeInformation, Double.class,
					parentDataHolder, fieldPos);
			} else if (typeClass.equals(String.class)) {
				return new RowConverter.BasicArrayConverter<String>((BasicArrayTypeInfo) typeInformation, String.class,
					parentDataHolder, fieldPos);
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
		private ParentDataHolder parentDataHolder;
		private int pos;

		RowPrimitiveConverter(ParentDataHolder parentDataHolder, int pos) {
			this.parentDataHolder = parentDataHolder;
			this.pos = pos;
		}

		@Override
		public void addBinary(Binary value) {
			parentDataHolder.add(pos, value.toStringUsingUTF8());
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
			parentDataHolder.add(pos, value);
		}

		@Override
		public void addLong(long value) {
			parentDataHolder.add(pos, value);
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
			parentDataHolder.add(pos, Iterables.<Row>toArray(list, Row.class));
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
		private final Class elementClass;
		private final int pos;
		private List<T> list;
		private Converter elementConverter;

		BasicArrayConverter(BasicArrayTypeInfo typeInfo, Class primitiveClass,
							ParentDataHolder parentDataHolder, int pos) {
			this.elementClass = primitiveClass;
			this.parentDataHolder = parentDataHolder;
			this.typeInfo = typeInfo;
			this.pos = pos;
			elementConverter = new RowConverter.RowPrimitiveConverter(this, 0);
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
			parentDataHolder.add(pos, Iterables.<T>toArray(list, elementClass));
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
