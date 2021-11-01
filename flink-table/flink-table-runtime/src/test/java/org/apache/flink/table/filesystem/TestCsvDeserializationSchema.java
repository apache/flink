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

package org.apache.flink.table.filesystem;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

/**
 * The {@link DeserializationSchema} that output {@link RowData}.
 *
 * <p>NOTE: This is meant only for testing purpose and doesn't provide a feature complete stable csv
 * parser! If you need a feature complete CSV parser, check out the flink-csv package.
 */
public class TestCsvDeserializationSchema implements DeserializationSchema<RowData> {

    @SuppressWarnings("rawtypes")
    private final DataStructureConverter[] csvRowToRowDataConverters;

    private final TypeInformation<RowData> typeInfo;
    private final int fieldCount;
    private final List<DataType> fieldTypes;

    private transient FieldParser<?>[] fieldParsers;

    public TestCsvDeserializationSchema(DataType dataType) {
        this.fieldTypes = DataType.getFieldDataTypes(dataType);
        this.fieldCount = fieldTypes.size();

        this.csvRowToRowDataConverters =
                fieldTypes.stream()
                        .map(DataStructureConverters::getConverter)
                        .toArray(DataStructureConverter[]::new);

        this.typeInfo = InternalTypeInfo.of((RowType) dataType.getLogicalType());

        initFieldParsers();
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        initFieldParsers();
    }

    @SuppressWarnings("unchecked")
    @Override
    public RowData deserialize(byte[] message) throws IOException {
        GenericRowData row = new GenericRowData(fieldCount);
        int startIndex = 0;
        for (int i = 0; i < fieldCount; i++) {
            startIndex =
                    this.fieldParsers[i].resetErrorStateAndParse(
                            message, startIndex, message.length, new byte[] {','}, null);
            row.setField(
                    i, csvRowToRowDataConverters[i].toInternal(fieldParsers[i].getLastResult()));
        }
        return row;
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInfo;
    }

    private void initFieldParsers() {
        this.fieldParsers = new FieldParser<?>[fieldCount];
        for (int i = 0; i < fieldTypes.size(); i++) {
            DataType fieldType = fieldTypes.get(i);
            Class<? extends FieldParser<?>> parserType =
                    FieldParser.getParserForType(
                            logicalTypeRootToFieldParserClass(
                                    fieldType.getLogicalType().getTypeRoot()));
            if (parserType == null) {
                throw new RuntimeException("No parser available for type '" + fieldType + "'.");
            }

            FieldParser<?> p = InstantiationUtil.instantiate(parserType, FieldParser.class);

            this.fieldParsers[i] = p;
        }
    }

    private Class<?> logicalTypeRootToFieldParserClass(LogicalTypeRoot root) {
        switch (root) {
            case CHAR:
            case VARCHAR:
                return String.class;
            case BOOLEAN:
                return Boolean.class;
            case DECIMAL:
                return BigDecimal.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case DATE:
                return java.sql.Date.class;
            case TIME_WITHOUT_TIME_ZONE:
                return java.sql.Time.class;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return java.sql.Timestamp.class;
            default:
                throw new RuntimeException(
                        "The provided type " + root + " is not supported by the testcsv format");
        }
    }
}
