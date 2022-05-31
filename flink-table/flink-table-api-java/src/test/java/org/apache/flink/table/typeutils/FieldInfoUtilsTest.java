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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.expressions.Expression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.assertj.core.api.Assertions.assertThat;

/** Test suite for {@link FieldInfoUtils}. */
class FieldInfoUtilsTest {

    private static Stream<TypeInformation> parameters() throws Exception {
        return Stream.of(
                new RowTypeInfo(
                        new TypeInformation[] {Types.INT, Types.LONG, Types.SQL_TIMESTAMP},
                        new String[] {"f0", "f1", "f2"}),
                new PojoTypeInfo(
                        MyPojo.class,
                        Arrays.asList(
                                new PojoField(MyPojo.class.getDeclaredField("f0"), Types.INT),
                                new PojoField(MyPojo.class.getDeclaredField("f1"), Types.LONG),
                                new PojoField(
                                        MyPojo.class.getDeclaredField("f2"),
                                        Types.SQL_TIMESTAMP))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void testByNameModeReorder(TypeInformation typeInfo) {
        FieldInfoUtils.TypeInfoSchema schema =
                FieldInfoUtils.getFieldsInfo(
                        typeInfo, new Expression[] {$("f2"), $("f1"), $("f0")});

        assertThat(schema.getFieldNames()).isEqualTo(new String[] {"f2", "f1", "f0"});
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void testByNameModeReorderAndRename(TypeInformation typeInfo) {
        FieldInfoUtils.TypeInfoSchema schema =
                FieldInfoUtils.getFieldsInfo(
                        typeInfo,
                        new Expression[] {$("f1").as("aa"), $("f0").as("bb"), $("f2").as("cc")});

        assertThat(schema.getFieldNames()).isEqualTo(new String[] {"aa", "bb", "cc"});
    }

    /** Test Pojo class. */
    public static class MyPojo {
        public int f0;
        public long f1;
        public Timestamp f2;

        public MyPojo() {}
    }

    private final RowTypeInfo rowTypeInfo =
            new RowTypeInfo(
                    new TypeInformation[] {Types.INT, Types.LONG, Types.SQL_TIMESTAMP},
                    new String[] {"f0", "f1", "f2"});

    @Test
    void testByPositionMode() {
        FieldInfoUtils.TypeInfoSchema schema =
                FieldInfoUtils.getFieldsInfo(
                        rowTypeInfo, new Expression[] {$("aa"), $("bb"), $("cc")});

        assertThat(schema.getFieldNames()).isEqualTo(new String[] {"aa", "bb", "cc"});
    }

    @Test
    void testByPositionModeProcTime() {
        FieldInfoUtils.TypeInfoSchema schema =
                FieldInfoUtils.getFieldsInfo(
                        rowTypeInfo,
                        new Expression[] {
                            $("aa"), $("bb"), $("cc"), $("cc").proctime().as("proctime")
                        });

        assertThat(schema.getFieldNames()).isEqualTo(new String[] {"aa", "bb", "cc", "proctime"});
    }
}
