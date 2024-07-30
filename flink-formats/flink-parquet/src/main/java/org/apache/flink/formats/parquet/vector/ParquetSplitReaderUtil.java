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

package org.apache.flink.formats.parquet.vector;

import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.formats.parquet.vector.reader.BooleanColumnReader;
import org.apache.flink.formats.parquet.vector.reader.ByteColumnReader;
import org.apache.flink.formats.parquet.vector.reader.BytesColumnReader;
import org.apache.flink.formats.parquet.vector.reader.ColumnReader;
import org.apache.flink.formats.parquet.vector.reader.DoubleColumnReader;
import org.apache.flink.formats.parquet.vector.reader.FixedLenBytesColumnReader;
import org.apache.flink.formats.parquet.vector.reader.FloatColumnReader;
import org.apache.flink.formats.parquet.vector.reader.IntColumnReader;
import org.apache.flink.formats.parquet.vector.reader.LongColumnReader;
import org.apache.flink.formats.parquet.vector.reader.NestedColumnReader;
import org.apache.flink.formats.parquet.vector.reader.ShortColumnReader;
import org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader;
import org.apache.flink.formats.parquet.vector.type.ParquetField;
import org.apache.flink.formats.parquet.vector.type.ParquetGroupField;
import org.apache.flink.formats.parquet.vector.type.ParquetPrimitiveField;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.data.columnar.vector.heap.HeapArrayVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapBooleanVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapByteVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapBytesVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapDoubleVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapFloatVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapIntVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapLongVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapMapVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapRowVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapShortVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapTimestampVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava32.com.google.common.collect.ImmutableList;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.utils.DateTimeUtils.toInternal;
import static org.apache.parquet.Preconditions.checkArgument;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/** Util for generating {@link ParquetColumnarRowSplitReader}. */
public class ParquetSplitReaderUtil {

    /** Util for generating partitioned {@link ParquetColumnarRowSplitReader}. */
    public static ParquetColumnarRowSplitReader genPartColumnarRowReader(
            boolean utcTimestamp,
            boolean caseSensitive,
            Configuration conf,
            String[] fullFieldNames,
            DataType[] fullFieldTypes,
            Map<String, Object> partitionSpec,
            int[] selectedFields,
            int batchSize,
            Path path,
            long splitStart,
            long splitLength)
            throws IOException {
        List<String> nonPartNames =
                Arrays.stream(fullFieldNames)
                        .filter(n -> !partitionSpec.containsKey(n))
                        .collect(Collectors.toList());

        List<String> selNonPartNames =
                Arrays.stream(selectedFields)
                        .mapToObj(i -> fullFieldNames[i])
                        .filter(nonPartNames::contains)
                        .collect(Collectors.toList());

        int[] selParquetFields = selNonPartNames.stream().mapToInt(nonPartNames::indexOf).toArray();

        ParquetColumnarRowSplitReader.ColumnBatchGenerator gen =
                readVectors -> {
                    // create and initialize the row batch
                    ColumnVector[] vectors = new ColumnVector[selectedFields.length];
                    for (int i = 0; i < vectors.length; i++) {
                        String name = fullFieldNames[selectedFields[i]];
                        LogicalType type = fullFieldTypes[selectedFields[i]].getLogicalType();
                        vectors[i] =
                                partitionSpec.containsKey(name)
                                        ? createVectorFromConstant(
                                                type, partitionSpec.get(name), batchSize)
                                        : readVectors[selNonPartNames.indexOf(name)];
                    }
                    return new VectorizedColumnBatch(vectors);
                };

        return new ParquetColumnarRowSplitReader(
                utcTimestamp,
                caseSensitive,
                conf,
                Arrays.stream(selParquetFields)
                        .mapToObj(i -> fullFieldTypes[i].getLogicalType())
                        .toArray(LogicalType[]::new),
                selNonPartNames.toArray(new String[0]),
                gen,
                batchSize,
                new org.apache.hadoop.fs.Path(path.toUri()),
                splitStart,
                splitLength);
    }

    public static ColumnVector createVectorFromConstant(
            LogicalType type, Object value, int batchSize) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                HeapBytesVector bsv = new HeapBytesVector(batchSize);
                if (value == null) {
                    bsv.fillWithNulls();
                } else {
                    bsv.fill(
                            value instanceof byte[]
                                    ? (byte[]) value
                                    : value.toString().getBytes(StandardCharsets.UTF_8));
                }
                return bsv;
            case BOOLEAN:
                HeapBooleanVector bv = new HeapBooleanVector(batchSize);
                if (value == null) {
                    bv.fillWithNulls();
                } else {
                    bv.fill((boolean) value);
                }
                return bv;
            case TINYINT:
                HeapByteVector byteVector = new HeapByteVector(batchSize);
                if (value == null) {
                    byteVector.fillWithNulls();
                } else {
                    byteVector.fill(((Number) value).byteValue());
                }
                return byteVector;
            case SMALLINT:
                HeapShortVector sv = new HeapShortVector(batchSize);
                if (value == null) {
                    sv.fillWithNulls();
                } else {
                    sv.fill(((Number) value).shortValue());
                }
                return sv;
            case INTEGER:
                HeapIntVector iv = new HeapIntVector(batchSize);
                if (value == null) {
                    iv.fillWithNulls();
                } else {
                    iv.fill(((Number) value).intValue());
                }
                return iv;
            case BIGINT:
                HeapLongVector lv = new HeapLongVector(batchSize);
                if (value == null) {
                    lv.fillWithNulls();
                } else {
                    lv.fill(((Number) value).longValue());
                }
                return lv;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                DecimalData decimal =
                        value == null
                                ? null
                                : Preconditions.checkNotNull(
                                        DecimalData.fromBigDecimal(
                                                (BigDecimal) value, precision, scale));
                ColumnVector internalVector;
                if (ParquetSchemaConverter.is32BitDecimal(precision)) {
                    internalVector =
                            createVectorFromConstant(
                                    new IntType(),
                                    decimal == null ? null : (int) decimal.toUnscaledLong(),
                                    batchSize);
                } else if (ParquetSchemaConverter.is64BitDecimal(precision)) {
                    internalVector =
                            createVectorFromConstant(
                                    new BigIntType(),
                                    decimal == null ? null : decimal.toUnscaledLong(),
                                    batchSize);
                } else {
                    internalVector =
                            createVectorFromConstant(
                                    new VarBinaryType(),
                                    decimal == null ? null : decimal.toUnscaledBytes(),
                                    batchSize);
                }
                return new ParquetDecimalVector(internalVector);
            case FLOAT:
                HeapFloatVector fv = new HeapFloatVector(batchSize);
                if (value == null) {
                    fv.fillWithNulls();
                } else {
                    fv.fill(((Number) value).floatValue());
                }
                return fv;
            case DOUBLE:
                HeapDoubleVector dv = new HeapDoubleVector(batchSize);
                if (value == null) {
                    dv.fillWithNulls();
                } else {
                    dv.fill(((Number) value).doubleValue());
                }
                return dv;
            case DATE:
                if (value instanceof LocalDate) {
                    value = Date.valueOf((LocalDate) value);
                }
                return createVectorFromConstant(
                        new IntType(), value == null ? null : toInternal((Date) value), batchSize);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                HeapTimestampVector tv = new HeapTimestampVector(batchSize);
                if (value == null) {
                    tv.fillWithNulls();
                } else {
                    tv.fill(TimestampData.fromLocalDateTime((LocalDateTime) value));
                }
                return tv;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static List<ColumnDescriptor> getAllColumnDescriptorByType(
            int depth, Type type, List<ColumnDescriptor> columns) throws ParquetRuntimeException {
        List<ColumnDescriptor> res = new ArrayList<>();
        for (ColumnDescriptor descriptor : columns) {
            if (depth >= descriptor.getPath().length) {
                throw new InvalidSchemaException("Corrupted Parquet schema");
            }
            if (type.getName().equals(descriptor.getPath()[depth])) {
                res.add(descriptor);
            }
        }

        // If doesn't find the type descriptor in corresponding depth, throw exception
        if (res.isEmpty()) {
            throw new InvalidSchemaException(
                    "Failed to find related Parquet column descriptor with type " + type);
        }
        return res;
    }

    public static ColumnReader createColumnReader(
            boolean isUtcTimestamp,
            LogicalType fieldType,
            Type type,
            List<ColumnDescriptor> columnDescriptors,
            PageReadStore pages,
            ParquetField field,
            int depth)
            throws IOException {
        List<ColumnDescriptor> descriptors =
                getAllColumnDescriptorByType(depth, type, columnDescriptors);
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return new BooleanColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case TINYINT:
                return new ByteColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case DOUBLE:
                return new DoubleColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case FLOAT:
                return new FloatColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return new IntColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case BIGINT:
                return new LongColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case SMALLINT:
                return new ShortColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return new BytesColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new TimestampColumnReader(
                        isUtcTimestamp,
                        descriptors.get(0),
                        pages.getPageReader(descriptors.get(0)));
            case DECIMAL:
                switch (descriptors.get(0).getPrimitiveType().getPrimitiveTypeName()) {
                    case INT32:
                        return new IntColumnReader(
                                descriptors.get(0), pages.getPageReader(descriptors.get(0)));
                    case INT64:
                        return new LongColumnReader(
                                descriptors.get(0), pages.getPageReader(descriptors.get(0)));
                    case BINARY:
                        return new BytesColumnReader(
                                descriptors.get(0), pages.getPageReader(descriptors.get(0)));
                    case FIXED_LEN_BYTE_ARRAY:
                        return new FixedLenBytesColumnReader(
                                descriptors.get(0),
                                pages.getPageReader(descriptors.get(0)),
                                ((DecimalType) fieldType).getPrecision());
                }
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
                return new NestedColumnReader(isUtcTimestamp, pages, field);
            default:
                throw new UnsupportedOperationException(fieldType + " is not supported now.");
        }
    }

    public static WritableColumnVector createWritableColumnVector(
            int batchSize,
            LogicalType fieldType,
            Type type,
            List<ColumnDescriptor> columnDescriptors,
            int depth) {
        List<ColumnDescriptor> descriptors =
                getAllColumnDescriptorByType(depth, type, columnDescriptors);
        PrimitiveType primitiveType = descriptors.get(0).getPrimitiveType();
        PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.BOOLEAN,
                        "Unexpected type: %s",
                        typeName);
                return new HeapBooleanVector(batchSize);
            case TINYINT:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.INT32,
                        "Unexpected type: %s",
                        typeName);
                return new HeapByteVector(batchSize);
            case DOUBLE:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.DOUBLE,
                        "Unexpected type: %s",
                        typeName);
                return new HeapDoubleVector(batchSize);
            case FLOAT:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.FLOAT,
                        "Unexpected type: %s",
                        typeName);
                return new HeapFloatVector(batchSize);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.INT32,
                        "Unexpected type: %s",
                        typeName);
                return new HeapIntVector(batchSize);
            case BIGINT:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.INT64,
                        "Unexpected type: %s",
                        typeName);
                return new HeapLongVector(batchSize);
            case SMALLINT:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.INT32,
                        "Unexpected type: %s",
                        typeName);
                return new HeapShortVector(batchSize);
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.BINARY,
                        "Unexpected type: %s",
                        typeName);
                return new HeapBytesVector(batchSize);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.INT96
                                || typeName == PrimitiveType.PrimitiveTypeName.INT64,
                        "Unexpected type: %s",
                        typeName);
                return new HeapTimestampVector(batchSize);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                if (ParquetSchemaConverter.is32BitDecimal(decimalType.getPrecision())) {
                    checkArgument(
                            (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                                            || typeName == PrimitiveType.PrimitiveTypeName.INT32)
                                    && primitiveType.getOriginalType() == OriginalType.DECIMAL,
                            "Unexpected type: %s",
                            typeName);
                    return new HeapIntVector(batchSize);
                } else if (ParquetSchemaConverter.is64BitDecimal(decimalType.getPrecision())) {
                    checkArgument(
                            (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                                            || typeName == PrimitiveType.PrimitiveTypeName.INT64)
                                    && primitiveType.getOriginalType() == OriginalType.DECIMAL,
                            "Unexpected type: %s",
                            typeName);
                    return new HeapLongVector(batchSize);
                } else {
                    checkArgument(
                            (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                                            || typeName == PrimitiveType.PrimitiveTypeName.BINARY)
                                    && primitiveType.getOriginalType() == OriginalType.DECIMAL,
                            "Unexpected type: %s",
                            typeName);
                    return new HeapBytesVector(batchSize);
                }
            case ARRAY:
                ArrayType arrayType = (ArrayType) fieldType;
                return new HeapArrayVector(
                        batchSize,
                        createWritableColumnVector(
                                batchSize,
                                arrayType.getElementType(),
                                type,
                                columnDescriptors,
                                depth));
            case MAP:
                MapType mapType = (MapType) fieldType;
                LogicalTypeAnnotation mapTypeAnnotation = type.getLogicalTypeAnnotation();
                GroupType mapRepeatedType = type.asGroupType().getType(0).asGroupType();
                if (mapTypeAnnotation.equals(LogicalTypeAnnotation.listType())) {
                    mapRepeatedType = mapRepeatedType.getType(0).asGroupType();
                    depth++;
                    if (mapRepeatedType
                            .getLogicalTypeAnnotation()
                            .equals(LogicalTypeAnnotation.mapType())) {
                        mapRepeatedType = mapRepeatedType.getType(0).asGroupType();
                        depth++;
                    }
                }
                return new HeapMapVector(
                        batchSize,
                        createWritableColumnVector(
                                batchSize,
                                mapType.getKeyType(),
                                mapRepeatedType.getType(0),
                                descriptors,
                                depth + 2),
                        createWritableColumnVector(
                                batchSize,
                                mapType.getValueType(),
                                mapRepeatedType.getType(1),
                                descriptors,
                                depth + 2));
            case MULTISET:
                MultisetType multisetType = (MultisetType) fieldType;
                LogicalTypeAnnotation multisetTypeAnnotation = type.getLogicalTypeAnnotation();
                GroupType multisetRepeatedType = type.asGroupType().getType(0).asGroupType();
                if (multisetTypeAnnotation.equals(LogicalTypeAnnotation.listType())) {
                    multisetRepeatedType = multisetRepeatedType.getType(0).asGroupType();
                    depth++;
                    if (multisetRepeatedType
                            .getLogicalTypeAnnotation()
                            .equals(LogicalTypeAnnotation.mapType())) {
                        multisetRepeatedType = multisetRepeatedType.getType(0).asGroupType();
                        depth++;
                    }
                }
                return new HeapMapVector(
                        batchSize,
                        createWritableColumnVector(
                                batchSize,
                                multisetType.getElementType(),
                                multisetRepeatedType.getType(0),
                                descriptors,
                                depth + 2),
                        createWritableColumnVector(
                                batchSize,
                                new IntType(false),
                                multisetRepeatedType.getType(1),
                                descriptors,
                                depth + 2));
            case ROW:
                RowType rowType = (RowType) fieldType;
                GroupType groupType = type.asGroupType();
                if (LogicalTypeAnnotation.listType().equals(groupType.getLogicalTypeAnnotation())) {
                    // this means there was two outside struct, need to get group twice.
                    groupType = groupType.getType(0).asGroupType();
                    groupType = groupType.getType(0).asGroupType();
                    depth = depth + 2;
                }
                WritableColumnVector[] columnVectors =
                        new WritableColumnVector[rowType.getFieldCount()];
                for (int i = 0; i < columnVectors.length; i++) {
                    columnVectors[i] =
                            createWritableColumnVector(
                                    batchSize,
                                    rowType.getTypeAt(i),
                                    groupType.getType(i),
                                    descriptors,
                                    depth + 1);
                }
                return new HeapRowVector(batchSize, columnVectors);
            default:
                throw new UnsupportedOperationException(fieldType + " is not supported now.");
        }
    }

    public static List<ParquetField> buildFieldsList(
            List<RowType.RowField> childrens, List<String> fieldNames, MessageColumnIO columnIO) {
        List<ParquetField> list = new ArrayList<>();
        for (int i = 0; i < childrens.size(); i++) {
            list.add(
                    constructField(
                            childrens.get(i), lookupColumnByName(columnIO, fieldNames.get(i))));
        }
        return list;
    }

    private static ParquetField constructField(RowType.RowField rowField, ColumnIO columnIO) {
        boolean required = columnIO.getType().getRepetition() == REQUIRED;
        int repetitionLevel = columnIO.getRepetitionLevel();
        int definitionLevel = columnIO.getDefinitionLevel();
        LogicalType type = rowField.getType();
        String filedName = rowField.getName();
        if (type instanceof RowType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            RowType rowType = (RowType) type;
            ImmutableList.Builder<ParquetField> fieldsBuilder = ImmutableList.builder();
            List<String> fieldNames = rowType.getFieldNames();
            List<RowType.RowField> childrens = rowType.getFields();
            for (int i = 0; i < childrens.size(); i++) {
                fieldsBuilder.add(
                        constructField(
                                childrens.get(i),
                                lookupColumnByName(groupColumnIO, fieldNames.get(i))));
            }

            return new ParquetGroupField(
                    type, repetitionLevel, definitionLevel, required, fieldsBuilder.build());
        }

        if (type instanceof MapType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            MapType mapType = (MapType) type;
            ParquetField keyField =
                    constructField(
                            new RowType.RowField("", mapType.getKeyType()),
                            keyValueColumnIO.getChild(0));
            ParquetField valueField =
                    constructField(
                            new RowType.RowField("", mapType.getValueType()),
                            keyValueColumnIO.getChild(1));
            return new ParquetGroupField(
                    type,
                    repetitionLevel,
                    definitionLevel,
                    required,
                    ImmutableList.of(keyField, valueField));
        }

        if (type instanceof MultisetType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            MultisetType multisetType = (MultisetType) type;
            ParquetField keyField =
                    constructField(
                            new RowType.RowField("", multisetType.getElementType()),
                            keyValueColumnIO.getChild(0));
            ParquetField valueField =
                    constructField(
                            new RowType.RowField("", new IntType()), keyValueColumnIO.getChild(1));
            return new ParquetGroupField(
                    type,
                    repetitionLevel,
                    definitionLevel,
                    required,
                    ImmutableList.of(keyField, valueField));
        }

        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            ColumnIO elementTypeColumnIO;
            if (columnIO instanceof GroupColumnIO) {
                GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
                if (!StringUtils.isNullOrWhitespaceOnly(filedName)) {
                    while (!Objects.equals(groupColumnIO.getName(), filedName)) {
                        groupColumnIO = (GroupColumnIO) groupColumnIO.getChild(0);
                    }
                    elementTypeColumnIO = groupColumnIO;
                } else {
                    if (arrayType.getElementType() instanceof RowType) {
                        elementTypeColumnIO = groupColumnIO;
                    } else {
                        elementTypeColumnIO = groupColumnIO.getChild(0);
                    }
                }
            } else if (columnIO instanceof PrimitiveColumnIO) {
                elementTypeColumnIO = columnIO;
            } else {
                throw new RuntimeException(String.format("Unknown ColumnIO, %s", columnIO));
            }

            ParquetField field =
                    constructField(
                            new RowType.RowField("", arrayType.getElementType()),
                            getArrayElementColumn(elementTypeColumnIO));
            if (repetitionLevel == field.getRepetitionLevel()) {
                repetitionLevel = columnIO.getParent().getRepetitionLevel();
            }
            return new ParquetGroupField(
                    type, repetitionLevel, definitionLevel, required, ImmutableList.of(field));
        }

        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        return new ParquetPrimitiveField(
                type, required, primitiveColumnIO.getColumnDescriptor(), primitiveColumnIO.getId());
    }

    /**
     * Parquet's column names are case in sensitive. So when we look up columns we first check for
     * exact match, and if that can not find we look for a case-insensitive match.
     */
    public static ColumnIO lookupColumnByName(GroupColumnIO groupColumnIO, String columnName) {
        ColumnIO columnIO = groupColumnIO.getChild(columnName);

        if (columnIO != null) {
            return columnIO;
        }

        for (int i = 0; i < groupColumnIO.getChildrenCount(); i++) {
            if (groupColumnIO.getChild(i).getName().equalsIgnoreCase(columnName)) {
                return groupColumnIO.getChild(i);
            }
        }

        throw new FlinkRuntimeException("Can not find column io for parquet reader.");
    }

    public static GroupColumnIO getMapKeyValueColumn(GroupColumnIO groupColumnIO) {
        while (groupColumnIO.getChildrenCount() == 1) {
            groupColumnIO = (GroupColumnIO) groupColumnIO.getChild(0);
        }
        return groupColumnIO;
    }

    public static ColumnIO getArrayElementColumn(ColumnIO columnIO) {
        while (columnIO instanceof GroupColumnIO && !columnIO.getType().isRepetition(REPEATED)) {
            columnIO = ((GroupColumnIO) columnIO).getChild(0);
        }

        /* Compatible with array has a standard 3-level structure:
         *  optional group my_list (LIST) {
         *     repeated group element {
         *        required binary str (UTF8);
         *     };
         *  }
         */
        if (columnIO instanceof GroupColumnIO
                && columnIO.getType().getLogicalTypeAnnotation() == null
                && ((GroupColumnIO) columnIO).getChildrenCount() == 1
                && !columnIO.getName().equals("array")
                && !columnIO.getName().equals(columnIO.getParent().getName() + "_tuple")) {
            return ((GroupColumnIO) columnIO).getChild(0);
        }

        /* Compatible with array for 2-level arrays where a repeated field is not a group:
         *   optional group my_list (LIST) {
         *      repeated int32 element;
         *   }
         */
        return columnIO;
    }
}
