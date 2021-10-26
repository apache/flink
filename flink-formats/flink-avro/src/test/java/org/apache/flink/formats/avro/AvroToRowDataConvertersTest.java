/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro;

import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link AvroToRowDataConverters}. */
public class AvroToRowDataConvertersTest {

    @Test
    public void testCreateRowConverter() {
        GenericRowData input = GenericRowData.of(1, StringData.fromString("test"), true, 10.26);
        RowType rowType =
                RowType.of(
                        new LogicalType[] {
                            DataTypes.INT().getLogicalType(),
                            DataTypes.STRING().getLogicalType(),
                            DataTypes.BOOLEAN().getLogicalType(),
                            DataTypes.DOUBLE().getLogicalType()
                        },
                        new String[] {"a", "b", "c", "d"});
        Schema schema = AvroSchemaConverter.convertToSchema(rowType);
        GenericRecord avroRecord =
                (GenericRecord)
                        RowDataToAvroConverters.createConverter(rowType).convert(schema, input);

        Assert.assertEquals(
                input, AvroToRowDataConverters.createRowConverter(rowType).convert(avroRecord));

        GenericRowData projectedRow = GenericRowData.of(true, StringData.fromString("test"));
        RowType projectedRowType =
                RowType.of(
                        new LogicalType[] {
                            DataTypes.BOOLEAN().getLogicalType(),
                            DataTypes.STRING().getLogicalType()
                        },
                        new String[] {"c", "b"});
        Assert.assertEquals(
                projectedRow,
                AvroToRowDataConverters.createRowConverter(projectedRowType, rowType)
                        .convert(avroRecord));
    }
}
