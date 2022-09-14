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
package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor;
import org.apache.flink.api.common.typeutils.TypeInformationTestBase;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RowTypeInfo}. */
class RowTypeInfoTest extends TypeInformationTestBase<RowTypeInfo> {
    private static TypeInformation<?>[] typeList =
            new TypeInformation<?>[] {
                BasicTypeInfo.INT_TYPE_INFO,
                new RowTypeInfo(BasicTypeInfo.SHORT_TYPE_INFO, BasicTypeInfo.BIG_DEC_TYPE_INFO),
                BasicTypeInfo.STRING_TYPE_INFO
            };

    @Override
    protected RowTypeInfo[] getTestData() {
        return new RowTypeInfo[] {
            new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
            new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.BOOLEAN_TYPE_INFO),
            new RowTypeInfo(typeList),
            new RowTypeInfo(
                    new TypeInformation[] {
                        BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO
                    },
                    new String[] {"int", "int2"})
        };
    }

    @Test
    void testWrongNumberOfFieldNames() {
        assertThatThrownBy(() -> new RowTypeInfo(typeList, new String[] {"int", "string"}))
                .isInstanceOf(IllegalArgumentException.class);
        // number of field names should be equal to number of types, go fail
    }

    @Test
    void testDuplicateCustomFieldNames() {
        assertThatThrownBy(
                        () -> new RowTypeInfo(typeList, new String[] {"int", "string", "string"}))
                .isInstanceOf(IllegalArgumentException.class);
        // field names should not be the same, go fail
    }

    @Test
    void testCustomFieldNames() {
        String[] fieldNames = new String[] {"int", "row", "string"};
        RowTypeInfo typeInfo1 = new RowTypeInfo(typeList, new String[] {"int", "row", "string"});
        assertThat(typeInfo1.getFieldNames()).isEqualTo(new String[] {"int", "row", "string"});

        assertThat(typeInfo1.getTypeAt("string")).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        assertThat(typeInfo1.getTypeAt(2)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        assertThat(typeInfo1.getTypeAt("row.0")).isEqualTo(BasicTypeInfo.SHORT_TYPE_INFO);
        assertThat(typeInfo1.getTypeAt("row.f1")).isEqualTo(BasicTypeInfo.BIG_DEC_TYPE_INFO);

        // change the names in fieldNames
        fieldNames[1] = "composite";
        RowTypeInfo typeInfo2 = new RowTypeInfo(typeList, fieldNames);
        // make sure the field names are deep copied
        assertThat(typeInfo1.getFieldNames()).isEqualTo(new String[] {"int", "row", "string"});
        assertThat(typeInfo2.getFieldNames())
                .isEqualTo(new String[] {"int", "composite", "string"});
    }

    @Test
    void testGetFlatFields() {
        RowTypeInfo typeInfo1 = new RowTypeInfo(typeList, new String[] {"int", "row", "string"});
        List<FlatFieldDescriptor> result = new ArrayList<>();
        typeInfo1.getFlatFields("row.*", 0, result);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).toString())
                .isEqualTo(new FlatFieldDescriptor(1, BasicTypeInfo.SHORT_TYPE_INFO).toString());
        assertThat(result.get(1).toString())
                .isEqualTo(new FlatFieldDescriptor(2, BasicTypeInfo.BIG_DEC_TYPE_INFO).toString());

        result.clear();
        typeInfo1.getFlatFields("string", 0, result);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).toString())
                .isEqualTo(new FlatFieldDescriptor(3, BasicTypeInfo.STRING_TYPE_INFO).toString());
    }

    @Test
    void testGetTypeAt() {
        RowTypeInfo typeInfo = new RowTypeInfo(typeList);

        assertThat(typeInfo.getFieldNames()).isEqualTo(new String[] {"f0", "f1", "f2"});

        assertThat(typeInfo.getTypeAt("f2")).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        assertThat(typeInfo.getTypeAt("f1.f0")).isEqualTo(BasicTypeInfo.SHORT_TYPE_INFO);
        assertThat(typeInfo.getTypeAt("f1.1")).isEqualTo(BasicTypeInfo.BIG_DEC_TYPE_INFO);
    }

    @Test
    void testNestedRowTypeInfo() {
        RowTypeInfo typeInfo = new RowTypeInfo(typeList);

        assertThat(typeInfo.getTypeAt("f1").toString()).isEqualTo("Row(f0: Short, f1: BigDecimal)");
        assertThat(typeInfo.getTypeAt("f1.f0").toString()).isEqualTo("Short");
    }

    @Test
    void testSchemaEquals() {
        final RowTypeInfo row1 =
                new RowTypeInfo(
                        new TypeInformation[] {
                            BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
                        },
                        new String[] {"field1", "field2"});
        final RowTypeInfo row2 =
                new RowTypeInfo(
                        new TypeInformation[] {
                            BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
                        },
                        new String[] {"field1", "field2"});
        assertThat(row1.schemaEquals(row2)).isTrue();

        final RowTypeInfo other1 =
                new RowTypeInfo(
                        new TypeInformation[] {
                            BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
                        },
                        new String[] {"otherField", "field2"});
        final RowTypeInfo other2 =
                new RowTypeInfo(
                        new TypeInformation[] {
                            BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
                        },
                        new String[] {"field1", "field2"});
        assertThat(row1.schemaEquals(other1)).isFalse();
        assertThat(row1.schemaEquals(other2)).isFalse();
    }
}
