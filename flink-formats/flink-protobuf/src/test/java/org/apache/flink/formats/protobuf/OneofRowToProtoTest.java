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

import org.apache.flink.formats.protobuf.testproto.OneofTest;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** Test conversion of flink internal map data to one_of proto data. */
public class OneofRowToProtoTest {
    @Test
    public void testSimple() throws Exception {
        RowData row = GenericRowData.of(1, 2);

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, OneofTest.class);
        OneofTest oneofTest = OneofTest.parseFrom(bytes);
        assertFalse(oneofTest.hasA());
        assertEquals(2, oneofTest.getB());
    }
}
