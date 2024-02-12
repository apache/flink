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

import org.apache.flink.formats.protobuf.testproto.VeryBigPbClass;

import org.junit.Test;

/**
 * Test for very huge proto definition, which may trigger some special optimizations such as code
 * splitting and java constant pool size optimization.
 *
 * <p>Implementing this test as an {@code ITCase} enables larger heap size for the test execution.
 * The current unit test execution configuration would cause {@code OutOfMemoryErrors}.
 */
public class VeryBigPbProtoToRowITCase {

    @Test
    public void testSimple() throws Exception {
        VeryBigPbClass.VeryBigPbMessage veryBigPbMessage =
                VeryBigPbClass.VeryBigPbMessage.newBuilder().build();
        // test generated code can be compiled
        ProtobufTestHelper.pbBytesToRow(
                VeryBigPbClass.VeryBigPbMessage.class, veryBigPbMessage.toByteArray());
    }
}
