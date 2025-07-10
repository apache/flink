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

package org.apache.flink.protobuf.registry.confluent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class ProtobufConfluentFormatFactoryUtilsTest {

    @Test
    void parseCustomProtoIncludes() {
        Assertions.assertTrue(
                ProtobufConfluentFormatFactoryUtils.parseCustomProtoIncludes("").isEmpty());

        List<String> expectedOne = new ArrayList<>();
        expectedOne.add("a");
        Assertions.assertEquals(
                expectedOne, ProtobufConfluentFormatFactoryUtils.parseCustomProtoIncludes("a"));

        List<String> expectedTwo = new ArrayList<>();
        expectedTwo.add("a");
        expectedTwo.add("b");
        Assertions.assertEquals(
                expectedTwo, ProtobufConfluentFormatFactoryUtils.parseCustomProtoIncludes("a,b"));
    }
}
