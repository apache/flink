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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
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

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.schema.Type.Repetition.REPEATED;

/** Extends from {@link GroupConverter} to convert an nested Parquet Record into Row. */
public class RowConverter extends GroupConverter implements ParentDataHolder {
    private final Converter[] converters;
    private final ParentDataHolder parentDataHolder;
    private final TypeInformation<?> typeInfo;
    private Row currentRow;
    private int posInParentRow;

    public RowConverter(MessageType messageType, TypeInformation<?> typeInfo) {
        this(messageType, typeInfo, null, 0);
    }

    public RowConverter(
            GroupType schema, TypeInformation<?> typeInfo, ParentDataHolder parent, int pos) {
        this.typeInfo = typeInfo;
        this.parentDataHolder = parent;
        this.posInParentRow = pos;
        this.converters = new Converter[schema.getFieldCount()];

        int i = 0;
        if (typeInfo.getArity() >= 1 && (typeInfo instanceof CompositeType)) {
            for (Type field : schema.getFields()) {
                converters[i] =
                        createConverter(field, i, ((CompositeType<?>) typeInfo).getTypeAt(i), this);
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
            return new RowConverter.MapConverter(
                    (GroupType) field, (MapTypeInfo) typeInformation, parentDataHolder, fieldPos);
        } else if (typeInformation instanceof BasicArrayTypeInfo) {
            Type elementType = field.asGroupType().getFields().get(0);
            Class typeClass =
                    ((BasicArrayTypeInfo) typeInformation).getComponentInfo().getTypeClass();

            if (typeClass.equals(Character.class)) {
                return new RowConverter.ArrayConverter<Character>(
                        elementType,
                        Character.class,
                        BasicTypeInfo.CHAR_TYPE_INFO,
                        parentDataHolder,
                        fieldPos);
            } else if (typeClass.equals(Boolean.class)) {
                return new RowConverter.ArrayConverter<Boolean>(
                        elementType,
                        Boolean.class,
                        BasicTypeInfo.BOOLEAN_TYPE_INFO,
                        parentDataHolder,
                        fieldPos);
            } else if (typeClass.equals(Short.class)) {
                return new RowConverter.ArrayConverter<Short>(
                        elementType,
                        Short.class,
                        BasicTypeInfo.SHORT_TYPE_INFO,
                        parentDataHolder,
                        fieldPos);
            } else if (typeClass.equals(Integer.class)) {
                return new RowConverter.ArrayConverter<Integer>(
                        elementType,
                        Integer.class,
                        BasicTypeInfo.INSTANT_TYPE_INFO,
                        parentDataHolder,
                        fieldPos);
            } else if (typeClass.equals(Long.class)) {
                return new RowConverter.ArrayConverter<Long>(
                        elementType,
                        Long.class,
                        BasicTypeInfo.LONG_TYPE_INFO,
                        parentDataHolder,
                        fieldPos);
            } else if (typeClass.equals(Double.class)) {
                return new RowConverter.ArrayConverter<Double>(
                        elementType,
                        Double.class,
                        BasicTypeInfo.DOUBLE_TYPE_INFO,
                        parentDataHolder,
                        fieldPos);
            } else if (typeClass.equals(String.class)) {
                return new RowConverter.ArrayConverter<String>(
                        elementType,
                        String.class,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        parentDataHolder,
                        fieldPos);
            } else if (typeClass.equals(Date.class)) {
                return new RowConverter.ArrayConverter<Date>(
                        elementType, Date.class, SqlTimeTypeInfo.DATE, parentDataHolder, fieldPos);
            } else if (typeClass.equals(Time.class)) {
                return new RowConverter.ArrayConverter<Time>(
                        elementType, Time.class, SqlTimeTypeInfo.TIME, parentDataHolder, fieldPos);
            } else if (typeClass.equals(Timestamp.class)) {
                return new RowConverter.ArrayConverter<Timestamp>(
                        elementType,
                        Timestamp.class,
                        SqlTimeTypeInfo.TIMESTAMP,
                        parentDataHolder,
                        fieldPos);
            } else if (typeClass.equals(BigDecimal.class)) {
                return new RowConverter.ArrayConverter<BigDecimal>(
                        elementType,
                        BigDecimal.class,
                        BasicTypeInfo.BIG_DEC_TYPE_INFO,
                        parentDataHolder,
                        fieldPos);
            }

            throw new IllegalArgumentException(
                    String.format(
                            "Can't create converter unsupported primitive array type for %s",
                            typeClass.toString()));

        } else if (typeInformation instanceof ObjectArrayTypeInfo) {
            GroupType parquetGroupType = field.asGroupType();
            Type elementType = parquetGroupType.getType(0);
            return new RowConverter.ArrayConverter<Row>(
                    elementType,
                    Row.class,
                    ((ObjectArrayTypeInfo) typeInformation).getComponentInfo(),
                    parentDataHolder,
                    fieldPos);
        } else if (typeInformation instanceof RowTypeInfo) {
            return new RowConverter((GroupType) field, typeInformation, parentDataHolder, fieldPos);
        }

        throw new IllegalArgumentException(
                String.format(
                        "Can't create converter for field %s with type %s ",
                        field.getName(), typeInformation.toString()));
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
        private OriginalType originalType;
        private PrimitiveType.PrimitiveTypeName primitiveTypeName;
        private ParentDataHolder parentDataHolder;
        private int pos;

        RowPrimitiveConverter(Type dataType, ParentDataHolder parentDataHolder, int pos) {
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
                parentDataHolder.add(
                        pos, new Timestamp(ParquetTimestampUtils.getTimestampMillis(value)));
                return;
            }

            if (originalType != null) {
                switch (originalType) {
                    case DECIMAL:
                        parentDataHolder.add(
                                pos, new BigDecimal(value.toStringUsingUTF8().toCharArray()));
                        break;
                    case UTF8:
                    case ENUM:
                    case JSON:
                    case BSON:
                        parentDataHolder.add(pos, value.toStringUsingUTF8());
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported original type : "
                                        + originalType.name()
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
                    case DECIMAL:
                        parentDataHolder.add(pos, value);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported original type : "
                                        + originalType.name()
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
                        throw new UnsupportedOperationException(
                                "Unsupported original type : "
                                        + originalType.name()
                                        + " for primitive type INT64");
                }
            } else {
                parentDataHolder.add(pos, value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    static class ArrayConverter<T> extends GroupConverter implements ParentDataHolder {
        private final ParentDataHolder parentDataHolder;
        private final Class elementClass;
        private final int pos;
        private List<T> list;
        private Converter elementConverter;

        ArrayConverter(
                Type elementType,
                Class elementClass,
                TypeInformation elementTypeInfo,
                ParentDataHolder parentDataHolder,
                int pos) {
            this.elementClass = elementClass;
            this.parentDataHolder = parentDataHolder;
            this.pos = pos;
            if (isElementType(elementType, elementTypeInfo)) {
                elementConverter =
                        createArrayElementConverter(
                                elementType, elementClass, elementTypeInfo, this);
            } else {
                GroupType groupType = elementType.asGroupType();
                elementConverter =
                        new ArrayElementConverter(groupType, elementClass, elementTypeInfo, this);
            }
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
            parentDataHolder.add(
                    pos, list.toArray((T[]) Array.newInstance(elementClass, list.size())));
        }

        @Override
        public void add(int fieldIndex, Object object) {
            list.add((T) object);
        }

        /**
         * Converter for list elements.
         *
         * <pre>
         *   optional group the_list (LIST) {
         *     repeated group array { <-- this layer
         *       optional (type) element;
         *     }
         *   }
         * </pre>
         */
        static class ArrayElementConverter extends GroupConverter {
            private final Converter elementConverter;

            public ArrayElementConverter(
                    GroupType repeatedTye,
                    Class elementClass,
                    TypeInformation elementTypeInfo,
                    ParentDataHolder holder) {
                Type elementType = repeatedTye.getType(0);
                this.elementConverter =
                        createArrayElementConverter(
                                elementType, elementClass, elementTypeInfo, holder);
            }

            @Override
            public Converter getConverter(int i) {
                return elementConverter;
            }

            @Override
            public void start() {}

            @Override
            public void end() {}
        }
    }

    static class MapConverter extends GroupConverter {
        private final ParentDataHolder parentDataHolder;
        private final Converter keyValueConverter;
        private final int pos;
        private Map<Object, Object> map;

        MapConverter(
                GroupType type, MapTypeInfo typeInfo, ParentDataHolder parentDataHolder, int pos) {
            this.parentDataHolder = parentDataHolder;
            this.pos = pos;
            this.keyValueConverter =
                    new MapKeyValueConverter((GroupType) type.getType(0), typeInfo);
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

            MapKeyValueConverter(GroupType groupType, MapTypeInfo typeInfo) {
                this.keyConverter =
                        createConverter(
                                groupType.getType(0),
                                0,
                                typeInfo.getKeyTypeInfo(),
                                (fieldIndex, object) -> key = object);

                this.valueConverter =
                        createConverter(
                                groupType.getType(1),
                                1,
                                typeInfo.getValueTypeInfo(),
                                (fieldIndex, object) -> value = object);
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

    static Converter createArrayElementConverter(
            Type elementType,
            Class elementClass,
            TypeInformation elementTypeInfo,
            ParentDataHolder holder) {
        if (elementClass.equals(Row.class)) {
            return createConverter(elementType, 0, elementTypeInfo, holder);
        } else {
            return new RowConverter.RowPrimitiveConverter(elementType, holder, 0);
        }
    }

    /**
     * Returns whether the given type is the element type of a list or is a synthetic group with one
     * field that is the element type. This is determined by checking whether the type can be a
     * synthetic group and by checking whether a potential synthetic group matches the expected
     * schema.
     *
     * @param repeatedType a type that may be the element type
     * @param typeInformation the expected flink type for list elements
     * @return {@code true} if the repeatedType is the element schema
     */
    static boolean isElementType(Type repeatedType, TypeInformation typeInformation) {
        if (repeatedType.isPrimitive()
                || repeatedType.asGroupType().getFieldCount() > 1
                || repeatedType.asGroupType().getType(0).isRepetition(REPEATED)) {
            // The repeated type must be the element type because it is an invalid
            // synthetic wrapper. Must be a group with one optional or required field
            return true;
        }

        return false;
    }
}
