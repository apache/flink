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

import org.apache.flink.formats.protobuf.proto.SimpleTestNoJavaPackage;
import org.apache.flink.formats.protobuf.util.PbToRowTypeUtil;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

/** test no java_package. */
public class NoJavaPackageProtoToRowTest {
    @Test
    public void testMessage() throws Exception {
        SimpleTestNoJavaPackage simple = SimpleTestNoJavaPackage.newBuilder().build();

        RowType schema = PbToRowTypeUtil.generateRowType(SimpleTestNoJavaPackage.getDescriptor());
        String[][] projectedField =
                new String[][] {
                    new String[] {"a"},
                    new String[] {"b"},
                    new String[] {"c"},
                    new String[] {"d"},
                    new String[] {"e"},
                    new String[] {"f"},
                    new String[] {"g"},
                    new String[] {"h"},
                    new String[] {"f_abc_7d"}
                };

        ProtobufTestProjectHelper.pbBytesToRowProjected(
                schema,
                simple.toByteArray(),
                new PbFormatConfig(SimpleTestNoJavaPackage.class.getName(), false, false, ""),
                projectedField);
    }
}
