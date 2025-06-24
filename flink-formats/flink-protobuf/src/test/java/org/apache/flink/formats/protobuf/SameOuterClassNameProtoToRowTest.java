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

package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.testproto.TestSameOuterClassNameOuterClass;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test conversion of proto same outer class name data to flink internal data. */
public class SameOuterClassNameProtoToRowTest {

    @Test
    public void testSimple() throws Exception {
        TestSameOuterClassNameOuterClass.TestSameOuterClassName testSameOuterClassName =
                TestSameOuterClassNameOuterClass.TestSameOuterClassName.newBuilder()
                        .setA(1)
                        .setB(TestSameOuterClassNameOuterClass.FooBar.BAR)
                        .build();
        RowData row =
                ProtobufTestHelper.pbBytesToRow(
                        TestSameOuterClassNameOuterClass.TestSameOuterClassName.class,
                        testSameOuterClassName.toByteArray());

        assertEquals(1, row.getInt(0));
        assertEquals("BAR", row.getString(1).toString());
    }

    @Test
    public void testIntEnum() throws Exception {
        TestSameOuterClassNameOuterClass.TestSameOuterClassName testSameOuterClassName =
                TestSameOuterClassNameOuterClass.TestSameOuterClassName.newBuilder()
                        .setB(TestSameOuterClassNameOuterClass.FooBar.BAR)
                        .build();

        RowData row =
                ProtobufTestHelper.pbBytesToRow(
                        TestSameOuterClassNameOuterClass.TestSameOuterClassName.class,
                        testSameOuterClassName.toByteArray(),
                        true);
        assertEquals(1, row.getInt(1));
    }
}
