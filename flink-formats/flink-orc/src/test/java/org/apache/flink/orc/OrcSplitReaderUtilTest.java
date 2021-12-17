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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.flink.orc.OrcSplitReaderUtil.logicalTypeToOrcType;

/** Test for {@link OrcSplitReaderUtil}. */
public class OrcSplitReaderUtilTest {

    @Test
    public void testLogicalTypeToOrcType() {
        test("boolean", DataTypes.BOOLEAN());
        test("char(123)", DataTypes.CHAR(123));
        test("varchar(123)", DataTypes.VARCHAR(123));
        test("string", DataTypes.STRING());
        test("binary", DataTypes.BYTES());
        test("tinyint", DataTypes.TINYINT());
        test("smallint", DataTypes.SMALLINT());
        test("int", DataTypes.INT());
        test("bigint", DataTypes.BIGINT());
        test("float", DataTypes.FLOAT());
        test("double", DataTypes.DOUBLE());
        test("date", DataTypes.DATE());
        test("timestamp", DataTypes.TIMESTAMP());
        test("array<float>", DataTypes.ARRAY(DataTypes.FLOAT()));
        test("map<float,bigint>", DataTypes.MAP(DataTypes.FLOAT(), DataTypes.BIGINT()));
        test(
                "struct<int0:int,str1:string,double2:double,row3:struct<int0:int,int1:int>>",
                DataTypes.ROW(
                        DataTypes.FIELD("int0", DataTypes.INT()),
                        DataTypes.FIELD("str1", DataTypes.STRING()),
                        DataTypes.FIELD("double2", DataTypes.DOUBLE()),
                        DataTypes.FIELD(
                                "row3",
                                DataTypes.ROW(
                                        DataTypes.FIELD("int0", DataTypes.INT()),
                                        DataTypes.FIELD("int1", DataTypes.INT())))));
        test("decimal(4,2)", DataTypes.DECIMAL(4, 2));
    }

    private void test(String expected, DataType type) {
        Assert.assertEquals(expected, logicalTypeToOrcType(type.getLogicalType()).toString());
    }
}
