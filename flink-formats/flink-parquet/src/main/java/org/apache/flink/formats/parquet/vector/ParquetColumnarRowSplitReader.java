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

import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.formats.parquet.vector.reader.ColumnReader;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil.createColumnReader;
import static org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil.createWritableColumnVector;
import static org.apache.flink.table.types.utils.DataTypeUtils.buildRow;
import static org.apache.flink.table.types.utils.DataTypeUtils.buildRowFields;
import static org.apache.flink.table.types.utils.DataTypeUtils.getFieldNameToIndex;
import static org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

/** This reader is used to read a {@link VectorizedColumnBatch} from input split. */
public class ParquetColumnarRowSplitReader implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetColumnarRowSplitReader.class);

    private final boolean utcTimestamp;

    private final MessageType fileSchema;

    private final MessageType requestedSchema;

    /**
     * The total number of rows this RecordReader will eventually read. The sum of the rows of all
     * the row groups.
     */
    private final long totalRowCount;

    private final WritableColumnVector[] writableVectors;

    private final VectorizedColumnBatch columnarBatch;

    private final ColumnarRowData row;

    private final LogicalType[] projectedTypes;

    private final int batchSize;

    private ParquetFileReader reader;

    /**
     * For each request column, the reader to read this column. This is NULL if this column is
     * missing from the file, in which case we populate the attribute with NULL.
     */
    private ColumnReader[] columnReaders;

    /** The number of rows that have been returned. */
    private long rowsReturned;

    /** The number of rows that have been reading, including the current in flight row group. */
    private long totalCountLoadedSoFar;

    // the index of the next row to return
    private int nextRow;

    // the number of rows in the current batch
    private int rowsInBatch;
    private final Set<Integer> unknownFieldsIndices = new HashSet<>();
    private final List<RowType.RowField> builtProjectedRowFields;
    private final RowType builtProjectedRowType;
    private final int[][] projectedFields;
    private final Map<String, Integer> fieldNameToIndex;

    public ParquetColumnarRowSplitReader(
            boolean utcTimestamp,
            boolean caseSensitive,
            Configuration conf,
            RowType fullType,
            RowType projectedType,
            int[][] projectedFields,
            ColumnBatchGenerator generator,
            int batchSize,
            Path path,
            long splitStart,
            long splitLength)
            throws IOException {
        this.utcTimestamp = utcTimestamp;
        this.projectedTypes = projectedType.getChildren().toArray(new LogicalType[0]);
        this.batchSize = batchSize;
        // projectedFields can be null when projectedType is not nested
        if (projectedFields == null) {
            this.projectedFields =
                    DataTypeUtils.computeProjectedFields(
                            projectedType.getFieldNames().toArray(new String[0]),
                            fullType.getFieldNames().toArray(new String[0]));
            this.builtProjectedRowFields = projectedType.getFields();
        } else {
            this.projectedFields = projectedFields;
            this.builtProjectedRowFields = buildRowFields(fullType, projectedFields);
        }
        this.builtProjectedRowType = buildRow(fullType, builtProjectedRowFields);
        this.fieldNameToIndex = getFieldNameToIndex(builtProjectedRowType.getFieldNames());

        // then we need to apply the predicate push down filter
        ParquetMetadata footer =
                readFooter(conf, path, range(splitStart, splitStart + splitLength));
        MessageType fileSchema = footer.getFileMetaData().getSchema();
        FilterCompat.Filter filter = getFilter(conf);
        List<BlockMetaData> blocks = filterRowGroups(filter, footer.getBlocks(), fileSchema);

        this.fileSchema = footer.getFileMetaData().getSchema();
        this.requestedSchema = clipParquetSchema(fileSchema, builtProjectedRowType, caseSensitive);
        this.reader =
                new ParquetFileReader(
                        conf, footer.getFileMetaData(), path, blocks, requestedSchema.getColumns());

        long totalRowCount = 0;
        for (BlockMetaData block : blocks) {
            totalRowCount += block.getRowCount();
        }
        this.totalRowCount = totalRowCount;
        this.nextRow = 0;
        this.rowsInBatch = 0;
        this.rowsReturned = 0;

        checkSchema();

        this.writableVectors = createWritableVectors();
        this.columnarBatch = generator.generate(createReadableVectors());
        this.row = new ColumnarRowData(columnarBatch);
    }

    /** Clips `parquetSchema` according to `projectedRowType`. */
    private MessageType clipParquetSchema(
            GroupType parquetSchema, RowType projectedRowType, boolean isCaseSensitive) {
        List<Type> clipParquetGroupFields =
                clipParquetGroupFields(parquetSchema, projectedRowType, isCaseSensitive);
        return Types.buildMessage()
                .addFields(clipParquetGroupFields.toArray(new Type[0]))
                .named("flink-parquet");
    }

    private Type clipParquetType(
            Type parquetSchema, LogicalType flinkType, boolean isCaseSensitive) {
        switch (flinkType.getTypeRoot()) {
            case ARRAY:
                ArrayType arrayType = (ArrayType) flinkType;
                if (!isPrimitiveType(arrayType.getElementType())) {
                    return clipParquetListType(
                            parquetSchema.asGroupType(),
                            arrayType.getElementType(),
                            isCaseSensitive);
                }
                return parquetSchema;
            case MAP:
                MapType mapType = (MapType) flinkType;
                if (!isPrimitiveType(mapType.getKeyType())
                        || !isPrimitiveType(mapType.getValueType())) {
                    return clipParquetMapType(
                            parquetSchema.asGroupType(),
                            mapType.getKeyType(),
                            mapType.getValueType(),
                            isCaseSensitive);
                }
                return parquetSchema;
            case ROW:
                return clipParquetGroup(
                        parquetSchema.asGroupType(), (RowType) flinkType, isCaseSensitive);
            default:
                return parquetSchema;
        }
    }

    private Type clipParquetListType(
            GroupType parquetList, LogicalType elementType, boolean isCaseSensitive) {
        assert !isPrimitiveType(elementType);
        // Unannotated repeated group should be interpreted as required list of required element, so
        // List element type is just the group itself.  Clip it
        if (parquetList.getLogicalTypeAnnotation() == null
                && parquetList.isRepetition(Type.Repetition.REPEATED)) {
            return clipParquetType(parquetList, elementType, isCaseSensitive);
        }
        if (!(parquetList.getLogicalTypeAnnotation()
                instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation)) {
            throw new FlinkRuntimeException(
                    "Invalid Parquet schema. Logical type annotation of annotated Parquet "
                            + "lists must be ListLogicalTypeAnnotation: "
                            + parquetList);
        }
        if (parquetList.getFieldCount() != 1
                || !parquetList.getType(0).isRepetition(Type.Repetition.REPEATED)) {
            throw new FlinkRuntimeException(
                    "Invalid Parquet schema. LIST-annotated group should only have exactly one repeated field: "
                            + parquetList);
        }
        Type firstType = parquetList.getType(0);
        assert !firstType.isPrimitive();
        GroupType repeatedGroup = firstType.asGroupType();
        // If the repeated field is a group with multiple fields, or the repeated field is a group
        // with one field and is named either "array" or uses the LIST-annotated group's name with
        // "_tuple" appended then the repeated type is the element type and elements are required.
        // Build a new LIST-annotated group with clipped `repeatedGroup` as element type and the
        // only field.
        if (repeatedGroup.getFieldCount() > 1
                || "array".equals(repeatedGroup.getName())
                || repeatedGroup.getName().equals(parquetList.getName() + "_tuple")) {
            return Types.buildGroup(parquetList.getRepetition())
                    .as(LogicalTypeAnnotation.listType())
                    .addField(clipParquetType(repeatedGroup, elementType, isCaseSensitive))
                    .named(parquetList.getName());
        } else {
            GroupType newRepeatedGroup =
                    Types.repeatedGroup()
                            .addField(
                                    clipParquetType(
                                            repeatedGroup.getType(0), elementType, isCaseSensitive))
                            .named(repeatedGroup.getName());
            // Otherwise, the repeated field's type is the element type with the repeated field's
            // repetition.
            return Types.buildGroup(parquetList.getRepetition())
                    .as(LogicalTypeAnnotation.listType())
                    .addField(newRepeatedGroup)
                    .named(parquetList.getName());
        }
    }

    private GroupType clipParquetMapType(
            GroupType parquetMap,
            LogicalType keyType,
            LogicalType valueType,
            boolean isCaseSensitive) {
        assert (!isPrimitiveType(keyType) || !isPrimitiveType(valueType));
        GroupType repeatedGroup = parquetMap.getType(0).asGroupType();
        Type parquetKeyType = repeatedGroup.getType(0);
        Type parquetValueType = repeatedGroup.getType(1);

        GroupType newRepeatedGroup =
                Types.repeatedGroup()
                        .as(repeatedGroup.getLogicalTypeAnnotation())
                        .addField(clipParquetType(parquetKeyType, keyType, isCaseSensitive))
                        .addField(clipParquetType(parquetValueType, valueType, isCaseSensitive))
                        .named(repeatedGroup.getName());
        return Types.buildGroup(parquetMap.getRepetition())
                .as(parquetMap.getLogicalTypeAnnotation())
                .addField(newRepeatedGroup)
                .named(parquetMap.getName());
    }

    private GroupType clipParquetGroup(
            GroupType parquetRow, RowType rowType, boolean isCaseSensitive) {
        List<Type> clippedParquetFields =
                clipParquetGroupFields(parquetRow, rowType, isCaseSensitive);
        return Types.buildGroup(parquetRow.getRepetition())
                .as(parquetRow.getLogicalTypeAnnotation())
                .addFields(clippedParquetFields.toArray(new Type[0]))
                .named(parquetRow.getName());
    }

    private List<Type> clipParquetGroupFields(
            GroupType parquetSchema, RowType projectedRowType, boolean isCaseSensitive) {
        if (isCaseSensitive) {
            return clipParquetGroupFieldsCaseSensitive(parquetSchema, projectedRowType);
        } else {
            return clipParquetGroupFieldsCaseInSensitive(parquetSchema, projectedRowType);
        }
    }

    private List<Type> clipParquetGroupFieldsCaseSensitive(
            GroupType parquetSchema, RowType projectedRowType) {
        List<Type> types = new ArrayList<>(projectedRowType.getFieldCount());
        List<String> fieldNames = projectedRowType.getFieldNames();
        List<LogicalType> projectedTypes = projectedRowType.getChildren();
        for (int i = 0; i < projectedRowType.getFieldCount(); ++i) {
            String fieldName = fieldNames.get(i);
            if (!parquetSchema.containsField(fieldName)) {
                LOG.warn(
                        "{} does not exist in {}, will fill the field with null.",
                        fieldName,
                        parquetSchema);
                types.add(
                        ParquetSchemaConverter.convertToParquetType(
                                fieldName, projectedTypes.get(i)));
                unknownFieldsIndices.add(i);
            } else {
                types.add(
                        clipParquetType(
                                parquetSchema.getType(fieldName), projectedTypes.get(i), true));
            }
        }
        return types;
    }

    private List<Type> clipParquetGroupFieldsCaseInSensitive(
            GroupType parquetSchema, RowType projectedRowType) {
        List<Type> types = new ArrayList<>(projectedRowType.getFieldCount());
        List<String> fieldNames = projectedRowType.getFieldNames();
        List<LogicalType> projectedTypes = projectedRowType.getChildren();
        Map<String, Type> caseInsensitiveFieldMap = new HashMap<>();
        for (Type type : parquetSchema.getFields()) {
            caseInsensitiveFieldMap.compute(
                    type.getName().toLowerCase(Locale.ROOT),
                    (key, previousType) -> {
                        if (previousType != null) {
                            throw new FlinkRuntimeException(
                                    "Parquet with case insensitive mode should have no duplicate key: "
                                            + key);
                        }
                        return type;
                    });
        }
        for (int i = 0; i < projectedRowType.getFieldCount(); ++i) {
            Type type = caseInsensitiveFieldMap.get(fieldNames.get(i).toLowerCase(Locale.ROOT));
            if (type == null) {
                LOG.warn(
                        "{} does not exist in {}, will fill the field with null.",
                        fieldNames.get(i),
                        parquetSchema);
                type =
                        ParquetSchemaConverter.convertToParquetType(
                                fieldNames.get(i).toLowerCase(Locale.ROOT), projectedTypes.get(i));
                unknownFieldsIndices.add(i);
                types.add(type);
            } else {
                types.add(clipParquetType(type, projectedTypes.get(i), false));
            }
        }
        return types;
    }

    private boolean isPrimitiveType(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case ROW:
            case MAP:
            case ARRAY:
                return false;
            default:
                return true;
        }
    }

    private WritableColumnVector[] createWritableVectors() {
        WritableColumnVector[] columns = new WritableColumnVector[builtProjectedRowFields.size()];
        List<Type> types = requestedSchema.getFields();
        for (int i = 0; i < builtProjectedRowFields.size(); i++) {
            int refIndex = fieldNameToIndex.get(builtProjectedRowFields.get(i).getName());
            columns[i] =
                    createWritableColumnVector(
                            batchSize,
                            builtProjectedRowFields.get(i).getType(),
                            types.get(refIndex),
                            requestedSchema.getColumns(),
                            projectedFields[i].length,
                            0);
        }
        return columns;
    }

    /**
     * Create readable vectors from writable vectors. Especially for decimal, see {@link
     * ParquetDecimalVector}.
     */
    private ColumnVector[] createReadableVectors() {
        ColumnVector[] vectors = new ColumnVector[writableVectors.length];
        for (int i = 0; i < writableVectors.length; i++) {
            vectors[i] =
                    projectedTypes[i].getTypeRoot() == LogicalTypeRoot.DECIMAL
                            ? new ParquetDecimalVector(writableVectors[i])
                            : writableVectors[i];
        }
        return vectors;
    }

    private void checkSchema() throws IOException, UnsupportedOperationException {
        if (builtProjectedRowType.getFieldCount() != requestedSchema.getFieldCount()) {
            throw new RuntimeException(
                    "The quality of field type is incompatible with the request schema!");
        }

        /*
         * Check that the requested schema is supported.
         */
        for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
            String[] colPath = requestedSchema.getPaths().get(i);
            if (fileSchema.containsPath(colPath)) {
                ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
                if (!fd.equals(requestedSchema.getColumns().get(i))) {
                    throw new UnsupportedOperationException("Schema evolution not supported.");
                }
            } else {
                if (requestedSchema.getColumns().get(i).getMaxDefinitionLevel() == 0) {
                    // Column is missing in data but the required data is non-nullable. This file is
                    // invalid.
                    throw new IOException(
                            "Required column is missing in data file. Col: "
                                    + Arrays.toString(colPath));
                }
            }
        }
    }

    /**
     * Method used to check if the end of the input is reached.
     *
     * @return True if the end is reached, otherwise false.
     * @throws IOException Thrown, if an I/O error occurred.
     */
    public boolean reachedEnd() throws IOException {
        return !ensureBatch();
    }

    public ColumnarRowData nextRecord() {
        // return the next row
        row.setRowId(this.nextRow++);
        return row;
    }

    /**
     * Checks if there is at least one row left in the batch to return. If no more row are
     * available, it reads another batch of rows.
     *
     * @return Returns true if there is one more row to return, false otherwise.
     * @throws IOException throw if an exception happens while reading a batch.
     */
    private boolean ensureBatch() throws IOException {
        if (nextRow >= rowsInBatch) {
            // Try to read the next batch if rows from the file.
            if (nextBatch()) {
                // No more rows available in the Rows array.
                nextRow = 0;
                return true;
            }
            return false;
        }
        // there is at least one Row left in the Rows array.
        return true;
    }

    /** Advances to the next batch of rows. Returns false if there are no more. */
    private boolean nextBatch() throws IOException {
        for (WritableColumnVector v : writableVectors) {
            v.reset();
        }
        columnarBatch.setNumRows(0);
        if (rowsReturned >= totalRowCount) {
            return false;
        }
        if (rowsReturned == totalCountLoadedSoFar) {
            readNextRowGroup();
        }

        int num = (int) Math.min(batchSize, totalCountLoadedSoFar - rowsReturned);
        for (int i = 0; i < columnReaders.length; ++i) {
            //noinspection unchecked
            columnReaders[i].readToVector(num, writableVectors[i]);
        }
        rowsReturned += num;
        columnarBatch.setNumRows(num);
        rowsInBatch = num;
        return true;
    }

    private void readNextRowGroup() throws IOException {
        PageReadStore pages = reader.readNextRowGroup();
        if (pages == null) {
            throw new IOException(
                    "expecting more rows but reached last block. Read "
                            + rowsReturned
                            + " out of "
                            + totalRowCount);
        }
        List<Type> types = requestedSchema.getFields();
        columnReaders = new ColumnReader[builtProjectedRowFields.size()];
        for (int i = 0; i < builtProjectedRowFields.size(); ++i) {
            int refIndex = fieldNameToIndex.get(builtProjectedRowFields.get(i).getName());
            if (!unknownFieldsIndices.contains(refIndex)) {
                columnReaders[i] =
                        createColumnReader(
                                utcTimestamp,
                                projectedTypes[i],
                                types.get(refIndex),
                                requestedSchema.getColumns(),
                                pages,
                                projectedFields[i].length,
                                0);
            }
        }
        totalCountLoadedSoFar += pages.getRowCount();
    }

    /** Seek to a particular row number. */
    public void seekToRow(long rowCount) throws IOException {
        if (totalCountLoadedSoFar != 0) {
            throw new UnsupportedOperationException("Only support seek at first.");
        }

        List<BlockMetaData> blockMetaData = reader.getRowGroups();

        for (BlockMetaData metaData : blockMetaData) {
            if (metaData.getRowCount() > rowCount) {
                break;
            } else {
                reader.skipNextRowGroup();
                rowsReturned += metaData.getRowCount();
                totalCountLoadedSoFar += metaData.getRowCount();
                rowsInBatch = (int) metaData.getRowCount();
                nextRow = (int) metaData.getRowCount();
                rowCount -= metaData.getRowCount();
            }
        }
        for (int i = 0; i < rowCount; i++) {
            boolean end = reachedEnd();
            if (end) {
                throw new RuntimeException("Seek to many rows.");
            }
            nextRecord();
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    /** Interface to gen {@link VectorizedColumnBatch}. */
    public interface ColumnBatchGenerator {
        VectorizedColumnBatch generate(ColumnVector[] readVectors);
    }
}
