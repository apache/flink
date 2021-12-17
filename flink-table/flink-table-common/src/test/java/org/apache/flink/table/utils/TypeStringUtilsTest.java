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

package org.apache.flink.table.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.ValidationException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link TypeStringUtils}. */
public class TypeStringUtilsTest {

    @Test
    public void testPrimitiveTypes() {
        testReadAndWrite("VARCHAR", Types.STRING);
        testReadAndWrite("BOOLEAN", Types.BOOLEAN);
        testReadAndWrite("TINYINT", Types.BYTE);
        testReadAndWrite("SMALLINT", Types.SHORT);
        testReadAndWrite("INT", Types.INT);
        testReadAndWrite("BIGINT", Types.LONG);
        testReadAndWrite("FLOAT", Types.FLOAT);
        testReadAndWrite("DOUBLE", Types.DOUBLE);
        testReadAndWrite("DECIMAL", Types.BIG_DEC);
        testReadAndWrite("DATE", Types.SQL_DATE);
        testReadAndWrite("TIME", Types.SQL_TIME);
        testReadAndWrite("TIMESTAMP", Types.SQL_TIMESTAMP);
        testWrite("DATE", Types.LOCAL_DATE);
        testWrite("TIME", Types.LOCAL_TIME);
        testWrite("TIMESTAMP", Types.LOCAL_DATE_TIME);

        // unsupported type information
        testReadAndWrite(
                "ANY<java.lang.Void, "
                        + "rO0ABXNyADJvcmcuYXBhY2hlLmZsaW5rLmFwaS5jb21tb24udHlwZWluZm8uQmFzaWNUeXBlSW5mb_oE8IKl"
                        + "ad0GAgAETAAFY2xhenp0ABFMamF2YS9sYW5nL0NsYXNzO0wAD2NvbXBhcmF0b3JDbGFzc3EAfgABWwAXcG9z"
                        + "c2libGVDYXN0VGFyZ2V0VHlwZXN0ABJbTGphdmEvbGFuZy9DbGFzcztMAApzZXJpYWxpemVydAA2TG9yZy9h"
                        + "cGFjaGUvZmxpbmsvYXBpL2NvbW1vbi90eXBldXRpbHMvVHlwZVNlcmlhbGl6ZXI7eHIANG9yZy5hcGFjaGUu"
                        + "ZmxpbmsuYXBpLmNvbW1vbi50eXBlaW5mby5UeXBlSW5mb3JtYXRpb26UjchIurN66wIAAHhwdnIADmphdmEu"
                        + "bGFuZy5Wb2lkAAAAAAAAAAAAAAB4cHB1cgASW0xqYXZhLmxhbmcuQ2xhc3M7qxbXrsvNWpkCAAB4cAAAAABz"
                        + "cgA5b3JnLmFwYWNoZS5mbGluay5hcGkuY29tbW9uLnR5cGV1dGlscy5iYXNlLlZvaWRTZXJpYWxpemVyAAAA"
                        + "AAAAAAECAAB4cgBCb3JnLmFwYWNoZS5mbGluay5hcGkuY29tbW9uLnR5cGV1dGlscy5iYXNlLlR5cGVTZXJp"
                        + "YWxpemVyU2luZ2xldG9ueamHqscud0UCAAB4cgA0b3JnLmFwYWNoZS5mbGluay5hcGkuY29tbW9uLnR5cGV1"
                        + "dGlscy5UeXBlU2VyaWFsaXplcgAAAAAAAAABAgAAeHA>",
                Types.VOID);
    }

    @Test
    public void testWriteComplexTypes() {
        testReadAndWrite("ROW<f0 DECIMAL, f1 TINYINT>", Types.ROW(Types.BIG_DEC, Types.BYTE));

        testReadAndWrite(
                "ROW<hello DECIMAL, world TINYINT>",
                Types.ROW_NAMED(new String[] {"hello", "world"}, Types.BIG_DEC, Types.BYTE));

        testReadAndWrite(
                "POJO<org.apache.flink.table.utils.TypeStringUtilsTest$TestPojo>",
                TypeExtractor.createTypeInfo(TestPojo.class));

        testReadAndWrite(
                "ANY<org.apache.flink.table.utils.TypeStringUtilsTest$TestNoPojo>",
                TypeExtractor.createTypeInfo(TestNoPojo.class));

        testReadAndWrite(
                "MAP<VARCHAR, ROW<f0 DECIMAL, f1 TINYINT>>",
                Types.MAP(Types.STRING, Types.ROW(Types.BIG_DEC, Types.BYTE)));

        testReadAndWrite(
                "MULTISET<ROW<f0 DECIMAL, f1 TINYINT>>",
                new MultisetTypeInfo<>(Types.ROW(Types.BIG_DEC, Types.BYTE)));

        testReadAndWrite("PRIMITIVE_ARRAY<TINYINT>", Types.PRIMITIVE_ARRAY(Types.BYTE));

        testReadAndWrite(
                "OBJECT_ARRAY<POJO<org.apache.flink.table.utils.TypeStringUtilsTest$TestPojo>>",
                Types.OBJECT_ARRAY(TypeExtractor.createTypeInfo(TestPojo.class)));

        // test escaping
        assertEquals(
                Types.ROW_NAMED(
                        new String[] {"he         \nllo", "world"}, Types.BIG_DEC, Types.BYTE),
                TypeStringUtils.readTypeInfo("ROW<`he         \nllo` DECIMAL, world TINYINT>"));

        assertEquals(
                Types.ROW_NAMED(new String[] {"he`llo", "world"}, Types.BIG_DEC, Types.BYTE),
                TypeStringUtils.readTypeInfo("ROW<`he``llo` DECIMAL, world TINYINT>"));

        // test backward compatibility with brackets ()
        assertEquals(
                Types.ROW_NAMED(
                        new String[] {"he         \nllo", "world"}, Types.BIG_DEC, Types.BYTE),
                TypeStringUtils.readTypeInfo("ROW(`he         \nllo` DECIMAL, world TINYINT)"));

        // test nesting
        testReadAndWrite(
                "ROW<singleton ROW<f0 INT>, twoField ROW<`Field 1` ROW<f0 DECIMAL>, `Field``s 2` VARCHAR>>",
                Types.ROW_NAMED(
                        new String[] {"singleton", "twoField"},
                        Types.ROW(Types.INT),
                        Types.ROW_NAMED(
                                new String[] {"Field 1", "Field`s 2"},
                                Types.ROW(Types.BIG_DEC),
                                Types.STRING)));

        testWrite(
                "ROW<f0 DECIMAL, f1 TIMESTAMP, f2 TIME, f3 DATE>",
                Types.ROW_NAMED(
                        new String[] {"f0", "f1", "f2", "f3"},
                        Types.BIG_DEC,
                        Types.LOCAL_DATE_TIME,
                        Types.LOCAL_TIME,
                        Types.LOCAL_DATE));
    }

    @Test(expected = ValidationException.class)
    public void testSyntaxError1() {
        TypeStringUtils.readTypeInfo("ROW<<f0 DECIMAL, f1 TINYINT>"); // additional <
    }

    @Test(expected = ValidationException.class)
    public void testSyntaxError2() {
        TypeStringUtils.readTypeInfo("ROW<f0 DECIMAL DECIMAL, f1 TINYINT>"); // duplicate type
    }

    @Test(expected = ValidationException.class)
    public void testSyntaxError3() {
        TypeStringUtils.readTypeInfo("ROW<f0 INVALID, f1 TINYINT>"); // invalid type
    }

    private void testReadAndWrite(String expected, TypeInformation<?> type) {
        // test read from string
        assertEquals(type, TypeStringUtils.readTypeInfo(expected));

        // test write to string
        assertEquals(expected, TypeStringUtils.writeTypeInfo(type));
    }

    private void testWrite(String expected, TypeInformation<?> type) {
        // test write to string
        assertEquals(expected, TypeStringUtils.writeTypeInfo(type));
    }

    // --------------------------------------------------------------------------------------------

    /** Test POJO. */
    public static class TestPojo {
        public int field1;
        public String field2;
    }

    /** Test invalid POJO. */
    public static class TestNoPojo {
        public int field1;
        public String field2;

        public TestNoPojo(int field1, String field2) { // no default constructor
            this.field1 = field1;
            this.field2 = field2;
        }
    }
}
