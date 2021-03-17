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

package org.apache.flink.orc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import org.apache.orc.TypeDescription;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link OrcBatchReader}. */
public class OrcBatchReaderTest {

    @Test
    public void testFlatSchemaToTypeInfo1() {

        String schema =
                "struct<"
                        + "boolean1:boolean,"
                        + "byte1:tinyint,"
                        + "short1:smallint,"
                        + "int1:int,"
                        + "long1:bigint,"
                        + "float1:float,"
                        + "double1:double,"
                        + "bytes1:binary,"
                        + "string1:string,"
                        + "date1:date,"
                        + "timestamp1:timestamp,"
                        + "decimal1:decimal(5,2)"
                        + ">";
        TypeInformation typeInfo =
                OrcBatchReader.schemaToTypeInfo(TypeDescription.fromString(schema));

        Assert.assertNotNull(typeInfo);
        Assert.assertTrue(typeInfo instanceof RowTypeInfo);
        RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInfo;

        // validate field types
        Assert.assertArrayEquals(
                new TypeInformation[] {
                    Types.BOOLEAN,
                    Types.BYTE,
                    Types.SHORT,
                    Types.INT,
                    Types.LONG,
                    Types.FLOAT,
                    Types.DOUBLE,
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
                    Types.STRING,
                    Types.SQL_DATE,
                    Types.SQL_TIMESTAMP,
                    BasicTypeInfo.BIG_DEC_TYPE_INFO
                },
                rowTypeInfo.getFieldTypes());

        // validate field names
        Assert.assertArrayEquals(
                new String[] {
                    "boolean1",
                    "byte1",
                    "short1",
                    "int1",
                    "long1",
                    "float1",
                    "double1",
                    "bytes1",
                    "string1",
                    "date1",
                    "timestamp1",
                    "decimal1"
                },
                rowTypeInfo.getFieldNames());
    }

    @Test
    public void testNestedSchemaToTypeInfo1() {

        String schema =
                "struct<"
                        + "middle:struct<"
                        + "list:array<"
                        + "struct<"
                        + "int1:int,"
                        + "string1:string"
                        + ">"
                        + ">"
                        + ">,"
                        + "list:array<"
                        + "struct<"
                        + "int1:int,"
                        + "string1:string"
                        + ">"
                        + ">,"
                        + "map:map<"
                        + "string,"
                        + "struct<"
                        + "int1:int,"
                        + "string1:string"
                        + ">"
                        + ">"
                        + ">";
        TypeInformation typeInfo =
                OrcBatchReader.schemaToTypeInfo(TypeDescription.fromString(schema));

        Assert.assertNotNull(typeInfo);
        Assert.assertTrue(typeInfo instanceof RowTypeInfo);
        RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInfo;

        // validate field types
        Assert.assertArrayEquals(
                new TypeInformation[] {
                    Types.ROW_NAMED(
                            new String[] {"list"},
                            ObjectArrayTypeInfo.getInfoFor(
                                    Types.ROW_NAMED(
                                            new String[] {"int1", "string1"},
                                            Types.INT,
                                            Types.STRING))),
                    ObjectArrayTypeInfo.getInfoFor(
                            Types.ROW_NAMED(
                                    new String[] {"int1", "string1"}, Types.INT, Types.STRING)),
                    new MapTypeInfo<>(
                            Types.STRING,
                            Types.ROW_NAMED(
                                    new String[] {"int1", "string1"}, Types.INT, Types.STRING))
                },
                rowTypeInfo.getFieldTypes());

        // validate field names
        Assert.assertArrayEquals(
                new String[] {"middle", "list", "map"}, rowTypeInfo.getFieldNames());
    }
}
