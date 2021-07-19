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

package org.apache.flink.formats.thrift;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.thrift.typeutils.ThriftUtils;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Serialization schema that serializes an object of Flink types into a THRIFT bytes.
 *
 * <p>Serializes the input Flink object into a THRIFT object and converts it into <code>byte[]
 * </code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link
 * ThriftRowDataDeserializationSchema}.
 */
public class ThriftRowDataSerializationSchema implements SerializationSchema<RowData> {

    /** The thrift class name for deserialize. */
    private String thriftClassName;

    /** The thrift protocol name. */
    private String thriftProtocolName;

    /** RowType to generate the converter. */
    private RowType rowType;

    /** The type of thrift class code generator. */
    private ThriftCodeGenerator codeGenerator;

    /** Flag indicating whether to fail if field mismatch. */
    private boolean ignoreFieldMismatch;

    /** Thrift class for serialization. */
    private transient Class<? extends TBase> thriftClass;

    /** Thrift serializer for serialization. */
    private transient TSerializer tSerializer;

    /** Runtime instance that performs the actual work. */
    private transient RowDataToThriftConverter converter;

    public ThriftRowDataSerializationSchema(
            RowType rowType,
            String thriftClassName,
            String thriftProtocolName,
            ThriftCodeGenerator codeGenerator,
            boolean ignoreFieldMismatch) {
        this.rowType = rowType;
        this.thriftClassName = thriftClassName;
        this.thriftProtocolName = thriftProtocolName;
        this.codeGenerator = codeGenerator;
        this.ignoreFieldMismatch = ignoreFieldMismatch;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        TProtocolFactory tProtocolFactory = ThriftUtils.getTProtocolFactory(thriftProtocolName);
        thriftClass =
                Thread.currentThread()
                        .getContextClassLoader()
                        .loadClass(thriftClassName)
                        .asSubclass(TBase.class);
        tSerializer = new TSerializer(tProtocolFactory);
        converter = createRowConverter(rowType, new StructMetaData(TType.STRUCT, thriftClass));
    }

    @Override
    public byte[] serialize(RowData element) {
        try {
            TBase thriftObject = (TBase) converter.convert(element);
            return tSerializer.serialize(thriftObject);
        } catch (Throwable t) {
            throw new RuntimeException(
                    "Could not serialize rowData '"
                            + element
                            + "'. "
                            + "Make sure that the schema matches the input.",
                    t);
        }
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(thriftClassName, thriftProtocolName);
        return result;
    }

    private interface RowDataToThriftConverter {
        Object convert(Object object);
    }

    private RowDataToThriftConverter createConverter(
            LogicalType type, FieldValueMetaData metaData) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGINT:
                return object -> object;
            case BINARY:
            case VARBINARY:
                if (codeGenerator == ThriftCodeGenerator.SCROOGE) {
                    return object -> ByteBuffer.wrap((byte[]) object);
                } else {
                    return object -> object;
                }
            case VARCHAR:
                if (metaData instanceof EnumMetaData) {
                    try {
                        Class enumClass =
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .loadClass(((EnumMetaData) metaData).enumClass.getName());
                        return object -> Enum.valueOf(enumClass, object.toString());
                    } catch (Exception e) {
                        throw new RuntimeException("Create convert error", e);
                    }
                }
                return object -> object.toString();
            case DECIMAL:
                return createDecimalConverter();
            case TIME_WITHOUT_TIME_ZONE:
            case DATE:
                return object -> (int) object;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return object -> {
                    TimestampData timestampData = (TimestampData) object;
                    return timestampData.toInstant().toEpochMilli();
                };
            case MULTISET:
            case MAP:
                return createMapConverter((MapType) type, (MapMetaData) metaData);
            case ARRAY:
                return createArrayConverter((ArrayType) type, (ListMetaData) metaData);
            case STRUCTURED_TYPE:
            case ROW:
                return createRowConverter((RowType) type, (StructMetaData) metaData);
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type.getTypeRoot());
        }
    }

    private RowDataToThriftConverter createDecimalConverter() {
        return object -> {
            DecimalData decimalData = (DecimalData) object;
            return decimalData.toUnscaledBytes();
        };
    }

    private RowDataToThriftConverter createArrayConverter(ArrayType type, ListMetaData metaData) {
        RowDataToThriftConverter valueConverter =
                createConverter(type.getElementType(), metaData.elemMetaData);
        ArrayData.ElementGetter elementGetter =
                ArrayData.createElementGetter(type.getElementType());

        return object -> {
            ArrayData arrayData = (ArrayData) object;
            List<Object> valueList = new ArrayList<>(arrayData.size());
            for (int i = 0; i < arrayData.size(); i++) {
                valueList.add(valueConverter.convert(elementGetter.getElementOrNull(arrayData, i)));
            }
            return valueList;
        };
    }

    private RowDataToThriftConverter createMapConverter(MapType type, MapMetaData metaData) {
        RowDataToThriftConverter keyConverter =
                createConverter(type.getKeyType(), metaData.keyMetaData);
        RowDataToThriftConverter valueConverter =
                createConverter(type.getValueType(), metaData.valueMetaData);
        ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(type.getKeyType());
        ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(type.getValueType());

        return object -> {
            MapData mapData = (MapData) object;
            ArrayData valueArray = mapData.valueArray();
            ArrayData keyArray = mapData.keyArray();
            Map<Object, Object> valueMap = new HashMap<>();
            for (int i = 0; i < keyArray.size(); i++) {
                Object key = keyConverter.convert(keyGetter.getElementOrNull(keyArray, i));
                Object value = valueConverter.convert(valueGetter.getElementOrNull(valueArray, i));
                valueMap.put(key, value);
            }

            return valueMap;
        };
    }

    private RowDataToThriftConverter createRowConverter(
            RowType rowType, StructMetaData structMetaData) {
        final Class<? extends TBase> tClass = structMetaData.structClass;
        final TFieldIdEnum[] fieldIdEnums =
                rowType.getFields().stream()
                        .map(RowType.RowField::getName)
                        .map(name -> ThriftUtils.getFieldIdEnum(tClass, name))
                        .toArray(TFieldIdEnum[]::new);
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[rowType.getFieldCount()];
        final RowDataToThriftConverter[] fieldConverts =
                new RowDataToThriftConverter[rowType.getFieldCount()];
        for (int i = 0; i < fieldConverts.length; ++i) {
            fieldConverts[i] =
                    createConverter(
                            rowType.getTypeAt(i),
                            ThriftUtils.getFieldMetaData(
                                    tClass, rowType.getFields().get(i).getName()));
            fieldGetters[i] = RowData.createFieldGetter(rowType.getTypeAt(i), i);
        }
        final int length = rowType.getFieldCount();

        final TBase reuse;
        try {
            reuse = tClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Unable to create row converter", e);
        }

        return object -> {
            RowData rowData = (RowData) object;
            for (int i = 0; i < length; ++i) {
                Object fieldValue = fieldGetters[i].getFieldOrNull(rowData);
                Object thriftValue = fieldConverts[i].convert(fieldValue);
                // if table schema has more fields than thrift class, fieldIdEnums[i] could be null
                if (fieldIdEnums[i] != null) {
                    reuse.setFieldValue(fieldIdEnums[i], thriftValue);
                } else {
                    if (!ignoreFieldMismatch) {
                        throw new RuntimeException("Table schema is not match the thrift class");
                    }
                }
            }
            return reuse;
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ThriftRowDataSerializationSchema that = (ThriftRowDataSerializationSchema) o;
        return thriftClassName.equals(that.thriftClassName)
                && thriftProtocolName.equals(that.thriftProtocolName)
                && rowType.equals(that.rowType);
    }
}
