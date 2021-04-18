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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.thrift.typeutils.ThriftUtils;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

/**
 * Deserialization schema from THRIFT to Flink Table/SQL internal data structure {@link RowData}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a THRIFT object and reads the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
public class ThriftRowDataDeserializationSchema implements DeserializationSchema<RowData> {
    private static final Logger LOG =
            LoggerFactory.getLogger(ThriftRowDataDeserializationSchema.class);

    /** The thrift class name for deserialize. */
    private String thriftClassName;

    /** The thrift protocol type class name . */
    private String thriftProtocolName;

    /** TypeInformation of the produced {@link RowData}. */
    private TypeInformation<RowData> typeInfo;

    /** RowType to generate the runtime converter. */
    private RowType rowType;

    /** Flag indicating whether to fail if parser error. */
    private boolean ignoreParseErrors;

    /** Flag indicating whether to fail if field mismatch. */
    private boolean ignoreFieldMismatch;

    /** Thrift deserializer. */
    private transient TDeserializer tDeserializer;

    /** The thrift class. */
    private transient Class<? extends TBase> thriftClass;

    /** A thrift instance for deserialize. */
    private transient TBase thriftInstance;

    /** Runtime instance that performs the actual work. */
    private transient ThriftToRowDataConverter converter;

    public ThriftRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> typeInfo,
            String thriftClassName,
            String thriftProtocolName,
            boolean ignoreParseErrors,
            boolean ignoreFieldMismatch) {
        this.rowType = rowType;
        this.typeInfo = typeInfo;
        this.thriftClassName = thriftClassName;
        this.thriftProtocolName = thriftProtocolName;
        this.ignoreParseErrors = ignoreParseErrors;
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
        thriftInstance = thriftClass.newInstance();
        tDeserializer = new TDeserializer(tProtocolFactory);
        converter = createRowConverter(rowType, new StructMetaData(TType.STRUCT, thriftClass));
    }

    private interface ThriftToRowDataConverter extends Serializable {
        Object convert(Object object);
    }

    private ThriftToRowDataConverter createConverter(
            LogicalType type, FieldValueMetaData metaData) {
        switch (type.getTypeRoot()) {
            case NULL:
                return thriftObject -> null;
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DOUBLE:
            case VARBINARY:
                return thriftObject -> thriftObject;
            case FLOAT:
                return thriftObject -> (float) thriftObject;
            case VARCHAR:
                if (metaData instanceof EnumMetaData) {
                    return thriftObject -> StringData.fromString(thriftObject.toString());
                } else {
                    return thriftObject -> StringData.fromString((String) thriftObject);
                }
            case DECIMAL:
                return createDecimalConverter((DecimalType) type, metaData);
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return thriftObject -> (int) thriftObject;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return thriftObject -> TimestampData.fromEpochMillis((long) thriftObject);
            case MULTISET:
            case MAP:
                return createMapConverter((MapType) type, (MapMetaData) metaData);
            case ARRAY:
                return createArrayConverter((ArrayType) type, metaData);
            case STRUCTURED_TYPE:
            case ROW:
                return createRowConverter((RowType) type, (StructMetaData) metaData);
            default:
                throw new RuntimeException("Unsupported type:" + type.getTypeRoot());
        }
    }

    private ThriftToRowDataConverter createDecimalConverter(
            DecimalType type, FieldValueMetaData metaData) {
        final int precision = type.getPrecision();
        final int scale = type.getScale();
        return object -> {
            byte[] bytes = (byte[]) object;
            return DecimalData.fromUnscaledBytes(bytes, precision, scale);
        };
    }

    private ThriftToRowDataConverter createArrayConverter(
            ArrayType type, FieldValueMetaData metaData) {
        if (metaData.type == TType.LIST) {
            ThriftToRowDataConverter valueConverter =
                    createConverter(type.getElementType(), ((ListMetaData) metaData).elemMetaData);
            return object -> {
                List<Object> valueList = (List<Object>) object;
                return new GenericArrayData(
                        valueList.stream().map(value -> valueConverter.convert(value)).toArray());
            };
        } else if (metaData.type == TType.SET) {
            ThriftToRowDataConverter valueConverter =
                    createConverter(type.getElementType(), ((ListMetaData) metaData).elemMetaData);
            return object -> {
                Set<Object> valueSet = (Set<Object>) object;
                return new GenericArrayData(
                        valueSet.stream().map(value -> valueConverter.convert(value)).toArray());
            };
        } else {
            throw new RuntimeException(
                    String.format("ArrayType is not match the thrift type:" + metaData.type));
        }
    }

    private ThriftToRowDataConverter createMapConverter(MapType type, MapMetaData metaData) {
        ThriftToRowDataConverter keyConverter =
                createConverter(type.getKeyType(), metaData.keyMetaData);
        ThriftToRowDataConverter valueConverter =
                createConverter(type.getValueType(), metaData.valueMetaData);
        return object -> {
            Map mapValue = new HashMap();
            Map<Object, Object> rowMap = (Map<Object, Object>) object;
            for (Map.Entry<Object, Object> entry : rowMap.entrySet()) {
                mapValue.put(
                        keyConverter.convert(entry.getKey()),
                        valueConverter.convert(entry.getValue()));
            }
            MapData mapData = new GenericMapData(mapValue);
            return mapData;
        };
    }

    private ThriftToRowDataConverter createNullableConverter(
            LogicalType type, FieldValueMetaData metadata) {
        if (metadata == null) {
            return thriftObject -> null;
        }

        final ThriftToRowDataConverter converter = createConverter(type, metadata);
        return thriftObject -> {
            if (thriftObject == null) {
                return null;
            }
            return converter.convert(thriftObject);
        };
    }

    private ThriftToRowDataConverter createRowConverter(
            RowType rowType, StructMetaData structMetaData) {
        Class<? extends TBase> tClass = structMetaData.structClass;
        final ThriftToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(
                                rowField ->
                                        createNullableConverter(
                                                rowField.getType(),
                                                ThriftUtils.getFieldMetaData(
                                                        tClass, rowField.getName())))
                        .toArray(ThriftToRowDataConverter[]::new);
        final TFieldIdEnum[] tFieldIdEnums =
                rowType.getFields().stream()
                        .map(RowType.RowField::getName)
                        .map(name -> ThriftUtils.getFieldIdEnum(tClass, name))
                        .toArray(TFieldIdEnum[]::new);
        final int arity = rowType.getFieldCount();

        return thriftObject -> {
            TBase tBase = (TBase) thriftObject;
            GenericRowData row = new GenericRowData(arity);
            for (int i = 0; i < arity; ++i) {
                TFieldIdEnum fieldIdEnum = tFieldIdEnums[i];
                // if table schema has more fields than thrift class, fieldIdEnums[i] could be null
                if (fieldIdEnum != null) {
                    row.setField(i, fieldConverters[i].convert(tBase.getFieldValue(fieldIdEnum)));
                } else {
                    if (!ignoreFieldMismatch) {
                        throw new RuntimeException("Table schema is not match the thrift class");
                    }
                }
            }
            return row;
        };
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        try {
            TBase tBase = thriftInstance.deepCopy();
            tDeserializer.deserialize(tBase, message);
            return (RowData) converter.convert(tBase);
        } catch (Exception e) {
            if (ignoreParseErrors && e instanceof TException) {
                LOG.warn(
                        format(
                                "Failed to deserialize Thrift object '%s'.",
                                thriftClass.getCanonicalName()),
                        e);
                return null;
            }
            throw new IOException(e);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ThriftRowDataDeserializationSchema that = (ThriftRowDataDeserializationSchema) o;

        return thriftClassName.equals(that.thriftClassName)
                && thriftProtocolName.equals(that.thriftProtocolName)
                && rowType.equals(that.rowType);
    }
}
